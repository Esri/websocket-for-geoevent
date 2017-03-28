/*
  Copyright 1995-2017 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
 */

package com.esri.geoevent.transport.websocket;

import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.property.Property;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

public class WebsocketInboundTransport extends InboundTransportBase
{

  private static final BundleLogger LOGGER                  = BundleLoggerFactory.getLogger(WebsocketInboundTransport.class);
  private static final String       URI_PROPERTY_NAME       = "URI";

  private static final int          DEFAULT_UNSECURE_PORT   = 80;
  private static final int          DEFAULT_SECURE_PORT     = 443;

  // default is 10 minutes
  public static final int           MAX_IDLE_TIME           = 10 * 60 * 1000;

  public static final int           MAX_TEXT_MESSAGE_SIZE   = 10 * 1024;
  public static final int           MAX_BINARY_MESSAGE_SIZE = 64 * 1024;

  private WebsocketInboundSocket    socket;
  private URI                       uri;
  private ByteBuffer                buffer                  = ByteBuffer.allocate(1024);
  private Charset                   charset;

  public WebsocketInboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
    charset = StandardCharsets.UTF_8;
  }

  public void start() throws RunningException
  {
    switch (getRunningState())
    {
      case STARTING:
      case STARTED:
        return;
      default:
    }

    setRunningState(RunningState.STARTING);
    Thread thread = new Thread()
    {
      @Override
      public void run()
      {
        setup();
      }
    };
    thread.start();
  }

  public synchronized void setup()
  {
    try
    {
      Property prop = getProperty(URI_PROPERTY_NAME);
      uri = getURI(prop.getValueAsString());
      socket = connect();
      setRunningState(RunningState.STARTED);
    }
    catch (ProtocolException error)
    {
      String errorMessage = LOGGER.translate("CANNOT_CONNECT", getProperty(URI_PROPERTY_NAME));
      LOGGER.error(errorMessage);
      LOGGER.info(error.getMessage(), error);
      setRunningState(RunningState.ERROR);
    }
    catch (Exception ex)
    {
      String errorMessage = LOGGER.translate("INIT_ERROR", ex.getMessage());
      LOGGER.error(errorMessage);
      LOGGER.info(ex.getMessage(), ex);
      setRunningState(RunningState.ERROR);
    }
  }

  private WebsocketInboundSocket connect() throws Exception
  {
	WebSocketClient wsc = null;
	  
	String uriStr = uri.toString();		
	if(uriStr.toLowerCase().startsWith("wss://")){		 
		SslContextFactory sslContextFactory = new SslContextFactory();
	    sslContextFactory.setTrustAll(true); 
	    wsc = new WebSocketClient(sslContextFactory);	      
	} else {
		wsc = new WebSocketClient();
	}
		  	      
    wsc.setMaxIdleTimeout(MAX_IDLE_TIME);
    wsc.setMaxTextMessageBufferSize(MAX_TEXT_MESSAGE_SIZE);

    WebsocketInboundSocket ws = new WebsocketInboundSocket(this);

    try
    {
      wsc.start();
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      Session session = wsc.connect(ws, uri, request).get();
      session.setIdleTimeout(MAX_IDLE_TIME);
    }
    catch (Throwable t)
    {
     LOGGER.error("WEBSOCKET_ERROR", t);            
    }
    return ws;
  }

  public void reconnect()
  {
    setRunningState(RunningState.ERROR);
    while (getRunningState() != RunningState.STOPPING && getRunningState() != RunningState.STOPPED)
    {
      try
      {
        socket = connect();
        if (socket.isConnected())
        {
          setRunningState(RunningState.STARTED);
          LOGGER.info("SOCKET_RECONNECTED");
          return;
        }
      }
      catch (Exception ex)
      {
        LOGGER.debug("RECONNECT_ERROR", ex);
        try
        {
          Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
        }
      }
    }
  }

  private URI getURI(String uriString) throws URISyntaxException
  {
    // check the port - default to 80 for unsecure and 443 for secure connections
    URIBuilder uriBuilder = new URIBuilder(uriString);
    if (uriBuilder.getPort() == -1 || uriBuilder.getPort() == 0)
    {
      if (StringUtils.isNotEmpty(uriBuilder.getScheme()))
      {
        if (uriBuilder.getScheme().equalsIgnoreCase("wss"))
        {
          uriBuilder.setPort(DEFAULT_SECURE_PORT);
        }
        else if (uriBuilder.getScheme().equalsIgnoreCase("ws"))
        {
          uriBuilder.setPort(DEFAULT_UNSECURE_PORT);
        }
      }
    }
    return uriBuilder.build();
  }

  public void receive(String dataString)
  {
    buffer.clear();
    buffer = charset.encode(dataString);
    byteListener.receive(buffer, null);
  }

  public synchronized void stop()
  {
    if (getRunningState() == RunningState.STOPPING || getRunningState() == RunningState.STOPPED)
      return;
    setRunningState(RunningState.STOPPING);

    if (socket != null)
    {
      socket.close();
      socket = null;
    }
    setRunningState(RunningState.STOPPED);
  }

  @Override
  public boolean isClusterable()
  {
    return false;
  }
}
