/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

public class ParseUrlFunction {

  @FunctionTemplate(name = "parse_url", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ParseUrl implements DrillSimpleFunc {

    @Param
    VarCharHolder in;
    @Output
    BaseWriter.ComplexWriter outWriter;
    @Inject
    DrillBuf outBuffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter urlMapWriter = outWriter.rootAsMap();

      String urlString =
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
      try {
        java.net.URL aURL = new java.net.URL(urlString);

        String protocol = aURL.getProtocol();
        String authority = aURL.getAuthority();
        String host = aURL.getHost();
        java.lang.Integer port = aURL.getPort();
        String path = aURL.getPath();
        String query = aURL.getQuery();
        String filename = aURL.getFile();
        String ref = aURL.getRef();

        org.apache.drill.exec.expr.holders.VarCharHolder rowHolder =
            new org.apache.drill.exec.expr.holders.VarCharHolder();

        urlMapWriter.start();

        byte[] protocolBytes = protocol.getBytes();
        outBuffer.reallocIfNeeded(protocolBytes.length);
        outBuffer.setBytes(0, protocolBytes);
        rowHolder.start = 0;
        rowHolder.end = protocolBytes.length;
        rowHolder.buffer = outBuffer;
        urlMapWriter.varChar("protocol").write(rowHolder);

        byte[] authorityBytes = authority.getBytes();
        outBuffer.reallocIfNeeded(authorityBytes.length);
        outBuffer.setBytes(0, authorityBytes);
        rowHolder.start = 0;
        rowHolder.end = authorityBytes.length;
        rowHolder.buffer = outBuffer;
        urlMapWriter.varChar("authority").write(rowHolder);

        byte[] hostBytes = host.getBytes();
        outBuffer.reallocIfNeeded(hostBytes.length);
        outBuffer.setBytes(0, hostBytes);
        rowHolder.start = 0;
        rowHolder.end = hostBytes.length;
        rowHolder.buffer = outBuffer;
        urlMapWriter.varChar("host").write(rowHolder);

        byte[] pathBytes = path.getBytes();
        outBuffer.reallocIfNeeded(pathBytes.length);
        outBuffer.setBytes(0, pathBytes);
        rowHolder.start = 0;
        rowHolder.end = pathBytes.length;
        rowHolder.buffer = outBuffer;
        urlMapWriter.varChar("path").write(rowHolder);

        if (query != null) {
          byte[] queryBytes = query.getBytes();
          outBuffer.reallocIfNeeded(queryBytes.length);
          outBuffer.setBytes(0, queryBytes);
          rowHolder.start = 0;
          rowHolder.end = queryBytes.length;
          rowHolder.buffer = outBuffer;
          urlMapWriter.varChar("query").write(rowHolder);
        }

        byte[] filenameBytes = filename.getBytes();
        outBuffer.reallocIfNeeded(filenameBytes.length);
        outBuffer.setBytes(0, filenameBytes);
        rowHolder.start = 0;
        rowHolder.end = filenameBytes.length;
        rowHolder.buffer = outBuffer;
        urlMapWriter.varChar("filename").write(rowHolder);

        if (ref != null) {
          byte[] refBytes = ref.getBytes();
          outBuffer.reallocIfNeeded(refBytes.length);
          outBuffer.setBytes(0, refBytes);
          rowHolder.start = 0;
          rowHolder.end = refBytes.length;
          rowHolder.buffer = outBuffer;
          urlMapWriter.varChar("ref").write(rowHolder);
        }

        if (port != -1) {
          org.apache.drill.exec.expr.holders.IntHolder intHolder = new org.apache.drill.exec.expr.holders.IntHolder();
          intHolder.value = port;
          urlMapWriter.integer("port").write(intHolder);
        }

        urlMapWriter.end();
      } catch (java.net.MalformedURLException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  @FunctionTemplate(name = "parse_url", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ParseUrlNullableInput implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder in;
    @Output
    BaseWriter.ComplexWriter outWriter;
    @Inject
    DrillBuf outBuffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mapWriter = outWriter.rootAsMap();

      if (in.isSet == 0) {
        // Return empty map
        mapWriter.start();
        mapWriter.end();
        return;
      }

      String urlString =
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
      try {
        java.net.URL aURL = new java.net.URL(urlString);

        String protocol = aURL.getProtocol();
        String authority = aURL.getAuthority();
        String host = aURL.getHost();
        java.lang.Integer port = aURL.getPort();
        String path = aURL.getPath();
        String query = aURL.getQuery();
        String filename = aURL.getFile();
        String ref = aURL.getRef();

        org.apache.drill.exec.expr.holders.VarCharHolder rowHolder =
            new org.apache.drill.exec.expr.holders.VarCharHolder();

        mapWriter.start();

        byte[] protocolBytes = protocol.getBytes();
        outBuffer.reallocIfNeeded(protocolBytes.length);
        outBuffer.setBytes(0, protocolBytes);
        rowHolder.start = 0;
        rowHolder.end = protocolBytes.length;
        rowHolder.buffer = outBuffer;
        mapWriter.varChar("protocol").write(rowHolder);

        byte[] authorityBytes = authority.getBytes();
        outBuffer.reallocIfNeeded(authorityBytes.length);
        outBuffer.setBytes(0, authorityBytes);
        rowHolder.start = 0;
        rowHolder.end = authorityBytes.length;
        rowHolder.buffer = outBuffer;
        mapWriter.varChar("authority").write(rowHolder);

        byte[] hostBytes = host.getBytes();
        outBuffer.reallocIfNeeded(hostBytes.length);
        outBuffer.setBytes(0, hostBytes);
        rowHolder.start = 0;
        rowHolder.end = hostBytes.length;
        rowHolder.buffer = outBuffer;
        mapWriter.varChar("host").write(rowHolder);

        byte[] pathBytes = path.getBytes();
        outBuffer.reallocIfNeeded(pathBytes.length);
        outBuffer.setBytes(0, pathBytes);
        rowHolder.start = 0;
        rowHolder.end = pathBytes.length;
        rowHolder.buffer = outBuffer;
        mapWriter.varChar("path").write(rowHolder);

        if (query != null) {
          byte[] queryBytes = query.getBytes();
          outBuffer.reallocIfNeeded(queryBytes.length);
          outBuffer.setBytes(0, queryBytes);
          rowHolder.start = 0;
          rowHolder.end = queryBytes.length;
          rowHolder.buffer = outBuffer;
          mapWriter.varChar("query").write(rowHolder);
        }

        byte[] filenameBytes = filename.getBytes();
        outBuffer.reallocIfNeeded(filenameBytes.length);
        outBuffer.setBytes(0, filenameBytes);
        rowHolder.start = 0;
        rowHolder.end = filenameBytes.length;
        rowHolder.buffer = outBuffer;
        mapWriter.varChar("filename").write(rowHolder);

        if (ref != null) {
          byte[] refBytes = ref.getBytes();
          outBuffer.reallocIfNeeded(refBytes.length);
          outBuffer.setBytes(0, refBytes);
          rowHolder.start = 0;
          rowHolder.end = refBytes.length;
          rowHolder.buffer = outBuffer;
          mapWriter.varChar("ref").write(rowHolder);
        }

        if (port != -1) {
          org.apache.drill.exec.expr.holders.IntHolder intHolder = new org.apache.drill.exec.expr.holders.IntHolder();
          intHolder.value = port;
          mapWriter.integer("port").write(intHolder);
        }

        mapWriter.end();
      } catch (java.net.MalformedURLException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
