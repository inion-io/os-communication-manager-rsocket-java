package io.inion.os.communication.manager.rsocket;

import io.inion.os.common.annotation.CellType;
import io.inion.os.common.communication.SiCommunicationManager;
import io.inion.os.common.communication.SiCommunicationMessage;
import io.inion.os.common.types.SiInteger;
import io.inion.os.common.types.SiString;
import io.inion.os.common.types.SiURI;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

@CellType(
    objectClass = SiCommunicationManagerRSocket.SiCommunicationManagerRSocketObject.class,
    type = SiCommunicationManagerRSocket.CELL_TYPE,
    uuid = SiCommunicationManagerRSocket.CELL_UUID
)
public interface SiCommunicationManagerRSocket extends
    SiCommunicationManager<SiCommunicationManagerRSocket> {

  class SiCommunicationManagerRSocketObject extends
      SiCommunicationManagerObject<SiCommunicationManagerRSocket> implements
      SiCommunicationManagerRSocket {

    @Override
    public void dispose() {

    }

    @Override
    public void listen(SiString host, SiInteger port) {
      // Hook to disable default error message
      // See: https://github.com/rsocket/rsocket-java/issues/1018
      // See: https://github.com/rsocket/rsocket-java/commit/1f7191456f2961d1d29f682fb609c7d0783ef9a2
      Hooks.onErrorDropped(e -> {
      });

      RSocket handler = new RSocket() {
        @Override
        public Mono<Void> fireAndForget(Payload payload) {
          System.out.println("FireAndForget: " + payload.getDataUtf8());
          return Mono.empty();
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
          try {
            SiCommunicationMessage request = createTransientCell(SiCommunicationMessage.class);
            request.restore(payload.getDataUtf8());

            SiCommunicationMessage response = processRequest(request);

            return Mono.just(DefaultPayload.create(response.toString()));
          } catch (Exception e) {
            log().error(e);
          }

          // TODO: Default error repsonse must send back
          return Mono.just(DefaultPayload.create(""));
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
          System.out.println("RequestStream: " + payload.getDataUtf8());
          return Flux.just("First", "Second").map(DefaultPayload::create);
        }
      };

      RSocketServer.create(SocketAcceptor.with(handler))
          .bind(TcpServerTransport.create(host.getCellValue(), port.getCellValue()))
          .block();
    }

    @Override
    public SiCommunicationMessage requestResponse(SiString host, SiInteger port,
        SiCommunicationMessage message) {

      AtomicReference<SiCommunicationMessage> response = new AtomicReference<>();

      try {
        RSocket socket =
            RSocketConnector.connectWith(
                TcpClientTransport.create(host.getCellValue(), port.getCellValue())).block();

        assert socket != null;
        socket
            .requestResponse(DefaultPayload.create(message.toString()))
            .map(Payload::getDataUtf8)
            .doOnError(onError -> {
              log().error(onError);
            })
            .onErrorReturn("{command: \"error\"}")
            .doOnNext(payload -> {
              SiCommunicationMessage responseMessage = createTransientCell(SiCommunicationMessage.class)
                  .restore(payload);

              //log().debug(responseMessage.toJsonString());

              response.set(processResponse(responseMessage));
            })
            .block();
        socket.dispose();
      } catch (Exception e) {
        // TODO: Lösung finden, um sauber damit zumzugehen, wenn auf dem Port nichts ist bzw. keine
        // Antworten kommen. Ansonsten wird das LOG vollgeschmiert
        //log().error(e);
      }

      return response.get();
    }

    @Override
    public SiCommunicationMessage requestResponse(SiURI cellURI, SiCommunicationMessage message) {
      SiString host = createTransientCell(SiString.class, cellURI.getCellValue().getHost());
      SiInteger port = createTransientCell(SiInteger.class, cellURI.getCellValue().getPort());

      return requestResponse(host, port, message);
    }
  }
}
