package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

/**
 * Test class to show how to use Mono and Flux.
 */
public class FluxFlow {

    // 1. stream of events using flux
    /** Returns the flux publisher. */
    public Flux<String> fluxPublisher() {
        return Flux
                .fromIterable(List.of("******Himansu******", "******Sanjay******"))
                .log();
    }

    // 2. transform event using map()
    /** Returns the flux transform using map publisher. */
    public Flux<String> fluxTransformUsingMapPublisher() {
        return Flux
                .fromIterable(List.of("******Himansu******", "******Sanjay******"))
                .map(String::toUpperCase)
                .map(name -> name.length() + "-" + name.toUpperCase())
                .log();
    }

    // 3. filter() stream of events
    /** Returns the flux filter publisher. */
    public Flux<String> fluxFilterPublisher(String name) {
        return Flux
                .fromIterable(List.of("******Himansu******", "******Sanjay******"))
                .filter(i -> i.toUpperCase().contains(name.toUpperCase()))
                .log();
    }

    // 4. flatMap() 2 streams H I M A N S U and N A Y A K to single flux stream
    /** Returns the flux flat map publisher. */
    public Flux<String> fluxFlatMapPublisher() {
        return Flux.fromIterable(List.of("Himansu", "Nayak"))
                .flatMap(name -> Flux.fromArray(name.split("")))
                .log();
    }

    // 5. flat map with delayElements()
    /** Returns the flux flat map delay publisher. */
    public Flux<String> fluxFlatMapDelayPublisher() {
        return Flux.fromIterable(List.of("Himansu", "Nayak"))
                .flatMap(name -> Flux.fromArray(name.split("")).delayElements(Duration.ofMillis(new Random().nextInt(1000))))
                .log();
    }

    // 6. concatMap() to maintain order of the flux stream event
    /** Returns the flux concat map delay publisher. */
    public Flux<String> fluxConcatMapDelayPublisher() {
        return Flux.fromIterable(List.of("Himansu", "Nayak"))
                .concatMap(name -> Flux.fromArray(name.split("")).delayElements(Duration.ofMillis(new Random().nextInt(1000))))
                .log();
    }

    // 7. transform()
    /** Returns the flux transform publisher. */
    public Flux<String> fluxTransformPublisher() {
        Function<Flux<String>, Flux<String>> mapFilterFunction = (name) -> name.map(String::toUpperCase).filter(i -> i.length() > 5);

        return Flux.fromIterable(List.of("Himansu", "Nayak"))
                .transform(mapFilterFunction)
                .log();
    }

    // 8. defaultIfEmpty()
    /** Returns the flux empty publisher. */
    public Flux<String> fluxEmptyPublisher() {
        return Flux.fromIterable(List.of("Himansu"))
                .filter(i -> i.length() > 10)
                .defaultIfEmpty("default")
                .log();
    }

    // 9. switchIfEmpty()
    /** Returns the flux switch if empty publisher. */
    public Flux<String> fluxSwitchIfEmptyPublisher() {
        Flux<String> just = Flux.just("default");
        return Flux.fromIterable(List.of("Himansu"))
                .filter(i -> i.length() > 10)
                .switchIfEmpty(just)
                .log();
    }
}
