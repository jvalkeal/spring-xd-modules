package org.springframework.xd.example;

import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.StringUtils;

public class ModifyMessageHandler extends AbstractReplyProducingMessageHandler {

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		String[] split = StringUtils.split((String)requestMessage.getPayload(), "\u0001");
		return MessageBuilder.fromMessage(requestMessage).setHeader("appkey", split[0]).build();
	}

	@Override
	protected boolean shouldCopyRequestHeaders() {
		return false;
	}

}
