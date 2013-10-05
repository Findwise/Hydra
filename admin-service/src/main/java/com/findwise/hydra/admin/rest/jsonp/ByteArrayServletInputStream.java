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

import java.io.IOException;
import javax.servlet.ServletInputStream;

/**
 *
 * @author ebbesson
 */
class ByteArrayServletInputStream extends ServletInputStream {
	protected byte[] buf;
	private int pos;
	private int mark = 0;
	private int count;

	public ByteArrayServletInputStream(final byte[] buf) {
		this.buf = buf;
		this.pos = 0;
		this.count = buf.length;
	}

	@Override
	public int read() throws IOException {
		return (pos < count) ? (buf[pos++] & 0xff) : -1;
	}

	@Override
	public long skip(final long n) {
		long k = count - pos;
		if (n < k) {
			k = n < 0 ? 0 : n;
		}
		pos += k;
		return k;
	}

	@Override
	public int available() {
		return count - pos;
	}

	@Override
	public boolean markSupported() {
		return true;
	}

	@Override
	public void mark(final int readAheadLimit) {
		mark = pos;
	}

	@Override
	public void reset() {
		pos = mark;
	}

	public int size() {
		return buf.length;
	}

}
