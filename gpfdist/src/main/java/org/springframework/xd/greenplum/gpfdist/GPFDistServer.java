/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.xd.greenplum.gpfdist;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import reactor.Environment;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.net.NetStreams;
import reactor.io.net.Spec.HttpServer;
import reactor.io.net.http.HttpChannel;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.SerializedBroadcaster;

public class GPFDistServer {

	private final static Log log = LogFactory.getLog(GPFDistServer.class);

	private final Processor<Buffer, Buffer> processor;

	private final int port;

	private final int flushCount;

	private final int flushTime;

	private final int batchTimeout;

	private final int batchCount;

	private reactor.io.net.http.HttpServer<Buffer, Buffer> server;

	public GPFDistServer(Processor<Buffer, Buffer> processor, int port, int flushCount, int flushTime, int batchTimeout, int batchCount) {
		this.processor = processor;
		this.port = port;
		this.flushCount = flushCount;
		this.flushTime = flushTime;
		this.batchTimeout = batchTimeout;
		this.batchCount = batchCount;
	}

	public synchronized reactor.io.net.http.HttpServer<Buffer, Buffer> start() throws Exception {
		if (server == null) {
			server = createProtocolListener();
		}
		return server;
	}

	public synchronized void stop() throws Exception {
		if (server != null) {
			server.shutdown().awaitSuccess();
		}
		server = null;
	}

	private reactor.io.net.http.HttpServer<Buffer, Buffer> createProtocolListener()
			throws Exception {

		final AtomicLong countedData = new AtomicLong();
		final AtomicLong countedBatchBefore = new AtomicLong();
		final AtomicLong countedBatchAfter = new AtomicLong();

		Streams.period(5, TimeUnit.SECONDS).consume(new Consumer<Long>() {

			@Override
			public void accept(Long t) {
				log.info("XXXX data=" + countedData.get() +" countedBatchBefore="+ countedBatchBefore.get() + " countedBatchAfter=" + countedBatchAfter);
			}
		});

		final Stream<Buffer> stream = Streams
		.wrap(processor)
		.observe(new Consumer<Buffer>() {
			@Override
			public void accept(Buffer t) {
				countedData.incrementAndGet();
			}
		})
		.window(flushCount, flushTime, TimeUnit.SECONDS)
		.flatMap(new Function<Stream<Buffer>, Publisher<Buffer>>() {

			@Override
			public Publisher<Buffer> apply(Stream<Buffer> t) {

//				Stream<Buffer> observe = t.observe(new Consumer<Buffer>() {
//					@Override
//					public void accept(Buffer t) {
//						countedData.incrementAndGet();
//					}
//				});

//				Stream<Buffer> reduce = observe.reduce(new Buffer(), new BiFunction<Buffer, Buffer, Buffer>() {
//
//					@Override
//					public Buffer apply(Buffer prev, Buffer next) {
//						return prev.append(next);
//					}
//				});

				Stream<Buffer> reduce = t.reduce(new Buffer(), new BiFunction<Buffer, Buffer, Buffer>() {

					@Override
					public Buffer apply(Buffer prev, Buffer next) {
						return prev.append(next);
					}
				});
//				return reduce.observe(new Consumer<Buffer>() {
//					@Override
//					public void accept(Buffer t) {
//						countedData.incrementAndGet();
//					}
//				});
				return reduce;
			}
		})
		.process(RingBufferWorkProcessor.<Buffer>create(false));

//		final Stream<Buffer> stream = Streams
//		.wrap(processor)
//		.window(flushCount, flushTime, TimeUnit.SECONDS)
//		.flatMap(new Function<Stream<Buffer>, Publisher<Buffer>>() {
//
//			@Override
//			public Publisher<Buffer> apply(Stream<Buffer> t) {
//				return t.reduce(new Buffer(), new BiFunction<Buffer, Buffer, Buffer>() {
//
//					@Override
//					public Buffer apply(Buffer prev, Buffer next) {
//						return prev.append(next);
//					}
//				});
//			}
//		})
//		.process(RingBufferWorkProcessor.<Buffer>create(false));

		reactor.io.net.http.HttpServer<Buffer, Buffer> httpServer = NetStreams
				.httpServer(new Function<HttpServer<Buffer, Buffer>, HttpServer<Buffer, Buffer>>() {

					@Override
					public HttpServer<Buffer, Buffer> apply(HttpServer<Buffer, Buffer> server) {
						return server
								.codec(new GPFDistCodec())
								.listen(port);
//								.dispatcher(Environment.sharedDispatcher());
					}
				});

		httpServer.get("/data", new Function<HttpChannel<Buffer, Buffer>, Publisher<Buffer>>(){

			@Override
			public Publisher<Buffer> apply(HttpChannel<Buffer, Buffer> request) {
				log.info("New incoming request: " + request.headers());
				request.responseHeaders().removeTransferEncodingChunked();
				request.addResponseHeader("Content-type", "text/plain");
				request.addResponseHeader("Expires", "0");
				request.addResponseHeader("X-GPFDIST-VERSION", "Spring XD");
				request.addResponseHeader("X-GP-PROTO", "1");
				request.addResponseHeader("Cache-Control", "no-cache");
				request.addResponseHeader("Connection", "close");

				return stream
						.observe(new Consumer<Buffer>() {
							@Override
							public void accept(Buffer t) {
								countedBatchBefore.incrementAndGet();
							}
						})
//						.log("before take")
//						.take(batchTime, TimeUnit.SECONDS)
//						.log("after take")
						.take(batchCount)
						.observe(new Consumer<Buffer>() {
							@Override
							public void accept(Buffer t) {
								countedBatchAfter.incrementAndGet();
							}
						})
//						.take(batchTimeout, TimeUnit.SECONDS)
//						.concatWith(Streams.just(Buffer.wrap(new byte[0])));

//						.take(batchCount)
						.timeout(batchTimeout, TimeUnit.SECONDS, Streams.<Buffer>empty())
						.concatWith(Streams.just(Buffer.wrap(new byte[0])));

//						.timeout(batchTimeout, TimeUnit.SECONDS, Streams.just(Buffer.wrap(new byte[0])).log("reactor.timeout"));
			}
		});

		httpServer.start().awaitSuccess();
		return httpServer;
	}

}
