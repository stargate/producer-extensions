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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CompletablePromise<V> extends CompletableFuture<V> {
  private Future<V> future;

  private CompletablePromise(Future<V> future) {
    this.future = future;
    CompletablePromiseContext.schedule(this::tryToComplete);
  }

  public static <V> CompletablePromise<V> fromFuture(Future<V> future) {
    return new CompletablePromise<>(future);
  }

  private void tryToComplete() {
    if (future.isDone()) {
      try {
        complete(future.get());
      } catch (InterruptedException e) {
        completeExceptionally(e);
      } catch (ExecutionException e) {
        completeExceptionally(e.getCause());
      }
      return;
    }

    if (future.isCancelled()) {
      cancel(true);
      return;
    }

    CompletablePromiseContext.schedule(this::tryToComplete);
  }
}
