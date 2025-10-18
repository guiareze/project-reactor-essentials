package br.com.guiareze;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {

    @Test
    public void fluxSubscriber() {
        Flux<String> fluxString = Flux.just("Guilherme", "Ana", "Maria")
                .log();

        StepVerifier.create(fluxString)
                .expectNext("Guilherme", "Ana", "Maria")
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbers() {
        Flux<Integer> fluxString = Flux.range(1,5)
                .log();

        fluxString.subscribe(i -> log.info("Number {}", i));

        log.info("-----------------------------------");

        StepVerifier.create(fluxString)
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberFromList() {
        Flux<Integer> fluxString = Flux.fromIterable(List.of(1,2,3,4,5))
                .log();

        fluxString.subscribe(i -> log.info("Number {}", i));

        log.info("-----------------------------------");

        StepVerifier.create(fluxString)
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersErrors() {
        Flux<Integer> fluxString = Flux.range(1,5)
                .log()
                .map(i -> {;
                    if (i == 4) {
                        throw new IndexOutOfBoundsException("Index out of bounds exception");
                    }
                    return i;
                });

        fluxString.subscribe(i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("DONE!"));

        log.info("-----------------------------------");

        StepVerifier.create(fluxString)
                .expectNext(1,2,3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberNumbersErrorsBackPressure() {
        Flux<Integer> fluxString = Flux.range(1,10)
                .log();

        fluxString.subscribe(i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("DONE!"),
                subscription -> subscription.request(3));

        log.info("-----------------------------------");

        StepVerifier.create(fluxString)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberNumbersErrorsAlmostCorrectBackPressure() {
        Flux<Integer> fluxString = Flux.range(1,10)
                .log();

        fluxString.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount){
                    count = 0;
                    log.info("Requesting more");
                    request(requestCount);
                }
            }
        });

        log.info("-----------------------------------");

        StepVerifier.create(fluxString)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersErrorsCorrectBackPressure() {
        Flux<Integer> fluxString = Flux.range(1,10)
                .log()
                .limitRate(3);

        fluxString.subscribe(i -> log.info("Number {}", i));

        log.info("-----------------------------------");

        StepVerifier.create(fluxString)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
    }

    @Test
    public void connectableFlux() throws Exception{
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::connect)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
    }

    @Test
    public void connectableFluxAutoConnect() throws Exception{
        Flux<Integer> connectableFlux = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::subscribe)
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

}
