package com.reactivespring;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class ServerSentEventTest {

    @Test
    @DisplayName("Test Server Sent Events using replay() Sink")
    public void serverSentEventsUsingSink() {
        //given
        // create publisher sink
        Sinks.Many<Integer> replaySink = Sinks.many().replay().all();

        //when
        // create data and emit integer 1 and 2 to sink
        replaySink.emitNext(1, Sinks.EmitFailureHandler.FAIL_FAST);
        replaySink.emitNext(2, Sinks.EmitFailureHandler.FAIL_FAST);

        //then
        // attach subscriber 1 to sink
        Flux<Integer> integerFlux = replaySink.asFlux();
        integerFlux.subscribe(i -> System.out.println("Subscriber 1 : " + i));

        // attach subscriber 2 to sink
        Flux<Integer> integerFlux2 = replaySink.asFlux();
        integerFlux2.subscribe(i -> System.out.println("Subscriber 2 : " + i));

        replaySink.tryEmitNext(3);
    }

    @Test
    @DisplayName("Test Server Sent Events using multicast() Sink")
    public void serverSentEventsUsingMultiCastSink() {
        //given
        // create publisher
        Sinks.Many<Integer> multiCast = Sinks.many().multicast().onBackpressureBuffer();

        //when
        // create data and emit integer 1 and 2
        multiCast.emitNext(1, Sinks.EmitFailureHandler.FAIL_FAST);
        multiCast.emitNext(2, Sinks.EmitFailureHandler.FAIL_FAST);


        //then
        // attach subscriber 1
        Flux<Integer> integerFlux = multiCast.asFlux();
        integerFlux.subscribe(i -> System.out.println("Subscriber 1 : " + i));

        // attach subscriber 2
        Flux<Integer> integerFlux2 = multiCast.asFlux();
        integerFlux2.subscribe(i -> System.out.println("Subscriber 2 : " + i));

        // create some more data
        multiCast.emitNext(3, Sinks.EmitFailureHandler.FAIL_FAST);
        multiCast.emitNext(3, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    @Test
    @DisplayName("Test Server Sent Events using unicast() Sink")
    public void serverSentEventsUsingUniCastSink() {
        //given
        // create publisher
        Sinks.Many<Integer> multiCast = Sinks.many().unicast().onBackpressureBuffer();

        //when
        // create data and emit integer 1 and 2
        multiCast.emitNext(1, Sinks.EmitFailureHandler.FAIL_FAST);
        multiCast.emitNext(2, Sinks.EmitFailureHandler.FAIL_FAST);


        //then
        // attach subscriber 1
        Flux<Integer> integerFlux = multiCast.asFlux();
        integerFlux.subscribe(i -> System.out.println("Subscriber 1 : " + i));

        // attach subscriber 2
        Flux<Integer> integerFlux2 = multiCast.asFlux();

        //java.lang.IllegalStateException: UnicastProcessor allows only a single Subscriber
        integerFlux2.subscribe(i -> System.out.println("Subscriber 2 : " + i));
    }


}
