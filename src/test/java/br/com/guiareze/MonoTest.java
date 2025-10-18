package br.com.guiareze;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
public class MonoTest {

    @Test
    public void monoSubscriber() {
        String name = "Guilherme Rezende";
        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe();
        log.info("---------------------");
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumer() {
        String name = "Guilherme Rezende";
        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe(s -> log.info("Value: {}", s));
        log.info("---------------------");
        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerComplete() {
        String name = "Guilherme Rezende";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));
        log.info("---------------------");
        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerSubscription() {
        String name = "Guilherme Rezende";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"),
                Subscription::cancel);
        log.info("---------------------");
        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerError() {
        String name = "Guilherme Rezende";
        Mono<String> mono = Mono.just(name)
                .map(s -> {
                    throw new RuntimeException("Testing mono with error");
                });

        mono.subscribe(s -> log.info("Name: {}", s), e -> log.error("Something bad happened: {}", e.getMessage()));
        mono.subscribe(s -> log.info("Name: {}", s), Throwable::printStackTrace);
        log.info("---------------------");
        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    public void monoDoOnMethod() {
        String name = "Guilherme Rezende";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(longNumber -> log.info("Request received, starting doing something..."))
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("This will not be printed"))
                .doOnSuccess(s -> log.info("The process is finished successfully: {}", s));

        mono.subscribe(s -> log.info("Value: {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));
        log.info("---------------------");
//        StepVerifier.create(mono)
//                .expectNext(name.toUpperCase())
//                .verifyComplete();
    }

    @Test
    public void monoDoOnError() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal Argument Exception"))
                .doOnError(s -> log.error("Error message: {}", s.getMessage()))
                .doOnNext(s -> log.info("This will not be printed"))
                .log();

        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    public void monoDoOnErrorResume() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal Argument Exception"))
                .doOnError(s -> log.error("Error message: {}", s.getMessage()))
                .onErrorResume(s -> {
                    log.info("Inside onErrorResume");
                    return Mono.just("EMPTY");
                })
                .log();

        StepVerifier.create(error)
                .expectNext("EMPTY")
                .verifyComplete();
    }

    @Test
    public void monoDoOnErrorReturn() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal Argument Exception"))
                .onErrorReturn("XPTO")
                .onErrorResume(s -> {
                    log.info("Inside onErrorResume");
                    return Mono.just("EMPTY");
                })
                .doOnError(s -> log.error("Error message: {}", s.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectNext("XPTO")
                .verifyComplete();
    }
}
