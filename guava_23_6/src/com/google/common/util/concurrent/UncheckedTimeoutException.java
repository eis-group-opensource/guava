/*
 * Copyright (C) 2006 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.util.concurrent;

import com.google.common.annotations.GwtIncompatible;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * Unchecked version of {@link java.util.concurrent.TimeoutException}.
 *
 * @author Kevin Bourrillion
 * @since 1.0
 */
@GwtIncompatible
public class UncheckedTimeoutException extends RuntimeException {
  public UncheckedTimeoutException() {}

  public UncheckedTimeoutException(@NullableDecl String message) {
    super(message);
  }

  public UncheckedTimeoutException(@NullableDecl Throwable cause) {
    super(cause);
  }

  public UncheckedTimeoutException(@NullableDecl String message, @NullableDecl Throwable cause) {
    super(message, cause);
  }

  private static final long serialVersionUID = 0;
}
