// Copyright (c) 2024, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:jni/jni.dart';
import 'package:web_socket/web_socket.dart';

import 'jni/bindings.dart' as bindings;
import 'ok_http_client.dart';

extension on List<int> {
  JByteArray toJByteArray() => JByteArray(length)..setRange(0, length, this);
}

/// A [WebSocket] implemented using the OkHttp library's
/// [WebSocket](https://square.github.io/okhttp/5.x/okhttp/okhttp3/-web-socket/index.html)
/// API.
///
/// > [!NOTE]
/// > The [WebSocket] interface is currently experimental and may change in the
/// > future.
///
/// Example usage of [OkHttpWebSocket]:
/// ```dart
/// import 'package:ok_http/ok_http.dart';
/// import 'package:web_socket/web_socket.dart';
///
/// void main() async {
///   final socket = await OkHttpWebSocket.connect(
///       Uri.parse('wss://ws.postman-echo.com/raw'));
///
///   socket.events.listen((e) async {
///     switch (e) {
///       case TextDataReceived(text: final text):
///         print('Received Text: $text');
///         await socket.close();
///       case BinaryDataReceived(data: final data):
///         print('Received Binary: $data');
///       case CloseReceived(code: final code, reason: final reason):
///         print('Connection to server closed: $code [$reason]');
///     }
///   });
/// }
/// ```
///
/// Example usage with custom configuration:
/// ```dart
/// import 'package:ok_http/ok_http.dart';
/// import 'package:web_socket/web_socket.dart';
///
/// void main() async {
///   final configuration = OkHttpClientConfiguration(
///     connectTimeout: Duration(seconds: 30),
///     readTimeout: Duration(seconds: 30),
///   );
///
///   final socket = await OkHttpWebSocket.connect(
///       Uri.parse('wss://ws.postman-echo.com/raw'),
///       configuration: configuration);
///
///   // Handle events...
/// }
/// ```
///
/// Example usage with custom OkHttpClient:
/// ```dart
/// import 'package:ok_http/ok_http.dart';
/// import 'package:web_socket/web_socket.dart';
///
/// void main() async {
///   final httpClient = OkHttpClient(
///     configuration: OkHttpClientConfiguration(
///       connectTimeout: Duration(seconds: 30),
///       readTimeout: Duration(seconds: 30),
///     ),
///   );
///
///   final socket = await OkHttpWebSocket.connect(
///       Uri.parse('wss://ws.postman-echo.com/raw'),
///       client: httpClient);
///
///   // Handle events...
/// }
/// ```
///
/// > [!TIP]
/// > [`AdapterWebSocketChannel`](https://pub.dev/documentation/web_socket_channel/latest/adapter_web_socket_channel/AdapterWebSocketChannel-class.html)
/// > can be used to adapt a [OkHttpWebSocket] into a
/// > [`WebSocketChannel`](https://pub.dev/documentation/web_socket_channel/latest/web_socket_channel/WebSocketChannel-class.html).
class OkHttpWebSocket implements WebSocket {
  late bindings.OkHttpClient _client;
  late final bindings.WebSocket _webSocket;
  final _events = StreamController<WebSocketEvent>();
  String? _protocol;
  final bool _ownsClient;

  /// Private constructor to prevent direct instantiation.
  ///
  /// Used by [connect] to create a new WebSocket connection, which requires a
  /// [bindings.OkHttpClient] instance (see [_connect]), and cannot be accessed
  /// statically.
  OkHttpWebSocket._({bindings.OkHttpClient? client, bool ownsClient = true})
      : _ownsClient = ownsClient {
    // Add the WebSocketInterceptor to prevent response parsing errors.
    _client = client ??
        bindings.WebSocketInterceptor.Companion
            .addWSInterceptor(bindings.OkHttpClient$Builder())
            .build();
  }

  /// Create a new WebSocket connection using `OkHttp`'s
  /// [WebSocket](https://square.github.io/okhttp/5.x/okhttp/okhttp3/-web-socket/index.html)
  /// API.
  ///
  /// The URL supplied in [url] must use the scheme ws or wss.
  ///
  /// If provided, the [protocols] argument indicates the subprotocols that
  /// the peer is able to select. See
  /// [RFC-6455 1.9](https://datatracker.ietf.org/doc/html/rfc6455#section-1.9).
  ///
  /// Optionally provide a [client] to use a specific [OkHttpClient] instance.
  /// If no client is provided, a new one will be created using the
  /// [configuration].
  ///
  /// The [configuration] is used to customize the OkHttpClient behavior when
  /// no [client] is provided. If both [client] and [configuration] are
  /// provided, the [configuration] is ignored.
  static Future<WebSocket> connect(
    Uri url, {
    Iterable<dynamic>? protocols,
    OkHttpClient? client,
    OkHttpClientConfiguration? configuration,
  }) async {
    final (underlyingClient, ownsClient) = _createClient(client, configuration);
    return OkHttpWebSocket._(
      client: underlyingClient,
      ownsClient: ownsClient,
    )._connect(url, protocols);
  }

  /// Creates the appropriate OkHttp client based on the provided parameters.
  /// 
  /// Returns a tuple of (underlyingClient, ownsClient) where:
  /// - underlyingClient: The bindings.OkHttpClient to use
  /// - ownsClient: Whether this WebSocket instance should own and close the
  ///   client
  static (bindings.OkHttpClient?, bool) _createClient(
    OkHttpClient? client,
    OkHttpClientConfiguration? configuration,
  ) {
    // If both client and configuration are provided, use the client and ignore
    // configuration
    if (client != null) {
      return (client.underlyingClient, false);
    }
    
    // If only configuration is provided, create a new client with it
    if (configuration != null) {
      final httpClient = OkHttpClient(configuration: configuration);
      return (httpClient.underlyingClient, true);
    }
    
    // Neither provided, use default behavior (client will be created in
    // constructor)
    return (null, true);
  }

  Future<WebSocket> _connect(Uri url, Iterable<dynamic>? protocols) async {
    if (!url.isScheme('ws') && !url.isScheme('wss')) {
      throw ArgumentError.value(
          url, 'url', 'only ws: and wss: schemes are supported');
    }
    final requestBuilder =
        bindings.Request$Builder().url$1(url.toString().toJString());

    if (protocols != null) {
      requestBuilder.addHeader('Sec-WebSocket-Protocol'.toJString(),
          protocols.join(', ').toJString());
    }

    var openCompleter = Completer<WebSocket>();

    _client.newWebSocket(
        requestBuilder.build(),
        bindings.WebSocketListenerProxy(
            bindings.WebSocketListenerProxy$WebSocketListener.implement(
                bindings.$WebSocketListenerProxy$WebSocketListener(
          onOpen: (webSocket, response) {
            _webSocket = webSocket;

            var protocolHeader =
                response.header$1('sec-websocket-protocol'.toJString());
            if (protocolHeader != null) {
              _protocol = protocolHeader.toDartString(releaseOriginal: true);
              if (!(protocols?.contains(_protocol) ?? true)) {
                openCompleter
                    .completeError(WebSocketException('Protocol mismatch. '
                        'Expected one of $protocols, but received $_protocol'));
                return;
              }
            }

            openCompleter.complete(this);
          },
          onMessage: (bindings.WebSocket webSocket, JString string) {
            if (_events.isClosed) return;
            _events.add(TextDataReceived(string.toDartString()));
          },
          onMessage$1:
              (bindings.WebSocket webSocket, bindings.ByteString byteString) {
            if (_events.isClosed) return;
            _events.add(BinaryDataReceived(
                Uint8List.fromList(byteString.toByteArray().toList())));
          },
          onClosing:
              (bindings.WebSocket webSocket, int i, JString string) async {
            _okHttpClientClose();

            if (_events.isClosed) return;

            _events.add(CloseReceived(i, string.toDartString()));
            await _events.close();
          },
          onFailure: (bindings.WebSocket webSocket, JObject throwable,
              bindings.Response? response) {
            if (_events.isClosed) return;

            var throwableString = throwable.toString();

            // If the throwable is:
            // - java.net.ProtocolException: Control frames must be final.
            // - java.io.EOFException
            // - java.net.SocketException: Socket closed
            // Then the connection was closed abnormally.
            if (throwableString.contains(RegExp(
                r'(java\.net\.ProtocolException: Control frames must be final\.|java\.io\.EOFException|java\.net\.SocketException: Socket closed)'))) {
              _events.add(CloseReceived(1006, 'abnormal close'));
              unawaited(_events.close());
              return;
            }
            var error = WebSocketException(
                'Connection ended unexpectedly $throwableString');
            if (openCompleter.isCompleted) {
              _events.addError(error);
              return;
            }
            openCompleter.completeError(error);
          },
        ))));

    return openCompleter.future;
  }

  @override
  Future<void> close([int? code, String? reason]) async {
    if (_events.isClosed) {
      throw WebSocketConnectionClosed();
    }

    if (code != null && code != 1000 && !(code >= 3000 && code <= 4999)) {
      throw ArgumentError('Invalid argument: $code, close code must be 1000 or '
          'in the range 3000-4999');
    }
    if (reason != null && utf8.encode(reason).length > 123) {
      throw ArgumentError.value(reason, 'reason',
          'reason must be <= 123 bytes long when encoded as UTF-8');
    }

    unawaited(_events.close());

    // When no code is provided, cause an abnormal closure to send 1005.
    if (code == null) {
      _webSocket.cancel();
      return;
    }

    _webSocket.close(code, reason?.toJString());
  }

  @override
  Stream<WebSocketEvent> get events => _events.stream;

  @override
  String get protocol => _protocol ?? '';

  @override
  void sendBytes(Uint8List b) {
    if (_events.isClosed) {
      throw WebSocketConnectionClosed();
    }
    _webSocket.send$1(bindings.ByteString.of(b.toJByteArray()));
  }

  @override
  void sendText(String s) {
    if (_events.isClosed) {
      throw WebSocketConnectionClosed();
    }
    _webSocket.send(s.toJString());
  }

  /// Closes the OkHttpClient using the recommended shutdown procedure.
  ///
  /// Only closes the client if this WebSocket instance owns it.
  /// If a custom client was provided, the client is not closed here
  /// since that's the responsibility of the client owner.
  ///
  /// https://square.github.io/okhttp/5.x/okhttp/okhttp3/-ok-http-client/index.html#:~:text=Shutdown
  void _okHttpClientClose() {
    if (_ownsClient) {
      _client.dispatcher().executorService().shutdown();
      _client.connectionPool().evictAll();
      var cache = _client.cache();
      if (cache != null) {
        cache.close();
      }
      _client.release();
    }
  }
}
