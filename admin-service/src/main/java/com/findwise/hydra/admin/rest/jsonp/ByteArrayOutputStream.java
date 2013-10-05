/*
 * Copyright 2013 ebbesson.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.findwise.hydra.admin.rest.jsonp;

import java.util.Arrays;
import javax.servlet.ServletOutputStream;

/**
 *
 * @author ebbesson
 */
class ByteArrayOutputStream extends ServletOutputStream {
	private byte[] buf = new byte[32];
	private int count;

	void ensureCapacity(final int minCapacity) {
		if (minCapacity - buf.length > 0) {
			grow(minCapacity);
		}
	}

	private void grow(final int minCapacity) {
		final int oldCapacity = buf.length;
		int newCapacity = oldCapacity << 1;
		if (newCapacity - minCapacity < 0) {
			newCapacity = minCapacity;
		}
		if (newCapacity < 0) {
			if (minCapacity < 0) {
				throw new OutOfMemoryError();
			}
			newCapacity = Integer.MAX_VALUE;
		}
		buf = Arrays.copyOf(buf, newCapacity);
	}

	@Override
	public void write(final int b) {
		ensureCapacity(count + 1);
		buf[count] = (byte) b;
		count += 1;
	}

	public void reset() {
		count = 0;
	}

	public byte[] toByteArray() {
		return Arrays.copyOf(buf, count);
	}

	public int size() {
		return count;
	}

}
