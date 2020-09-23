/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka.converters.future;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class CompletablePromiseTest {

  private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

  @Test
  public void shouldConvertFutureToCompletableFuture()
      throws ExecutionException, InterruptedException {
    // given
    Future<Integer> future = EXECUTOR.submit(() -> 1);

    // when
    CompletablePromise<Integer> completablePromise = CompletablePromise.fromFuture(future);

    // then
    assertThat(completablePromise.get()).isEqualTo(1);
  }

  @Test
  public void shouldPropagateErrorToCompletableFuture() {
    // given
    Future<?> future =
        EXECUTOR.submit(
            () -> {
              throw new RuntimeException("propagated");
            });

    // when
    CompletablePromise<?> completablePromise = CompletablePromise.fromFuture(future);

    // then
    assertThatThrownBy(completablePromise::get)
        .isInstanceOf(ExecutionException.class)
        .hasRootCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("propagated");
  }

  @Test
  public void shouldTimeoutIfUnderlyingFutureDoesNotComplete() {
    // given
    Future<?> future =
        EXECUTOR.submit(
            () -> {
              try {
                // sleep for longer period that timeout used in blocking get()
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            });

    // when
    CompletablePromise<?> completablePromise = CompletablePromise.fromFuture(future);

    // then
    assertThatThrownBy(() -> completablePromise.get(10, TimeUnit.MILLISECONDS))
        .isInstanceOf(TimeoutException.class);
  }
}
