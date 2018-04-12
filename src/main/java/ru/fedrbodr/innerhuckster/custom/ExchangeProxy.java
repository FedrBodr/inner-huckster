package ru.fedrbodr.innerhuckster.custom;

import lombok.extern.slf4j.Slf4j;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.service.marketdata.MarketDataService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class ExchangeProxy {

	private List<Exchange> exchangePoll = new ArrayList<>();
	private List<MarketDataService> marketDataServicePoll = new ArrayList<>();
	private Integer lastExchangeProsyUsedNum = 0;

	public ExchangeProxy(List<String> proxyHostPortList, String exchangeClassName) {
		ExecutorService executorService = Executors.newFixedThreadPool(proxyHostPortList.size());
		try {
			for (String proxyHostAndPort : proxyHostPortList) {
				Callable<Void> tCallable = () -> {
					try {
						String[] split = proxyHostAndPort.split(":");
						exchangePoll.add(getExchangeProxy(split[0], Integer.parseInt(split[1]), exchangeClassName));
						return null;
					} catch (Exception e) {
						log.error("Init proxy error " + proxyHostAndPort +" for exchange "+ exchangeClassName, e);
						return null;
					}
				};
				executorService.execute(new FutureTask(tCallable));
			}
			executorService.shutdown();
			try {
				executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				log.error("Init proxy awaitTermination error " + e.getMessage(), e);
			}
			if(exchangePoll.size()==0){
				throw new IllegalArgumentException("Problem proxy list init - empty exchangePoll recognized.");
			}
		} catch (Exception e) {
			throw new IllegalArgumentException("Proxy list in incorrect format! Must be proxy.list: 185.128.215.224:8000,193.93.60.95:8000,193.93.60.236:8000");
		}
	}

	public MarketDataService getNextMarketDataService() {
		MarketDataService nextExchangeProxyForUse = marketDataServicePoll.get(0);

		return nextExchangeProxyForUse;
	}

	public Exchange getNextExchange() {
		Exchange nextExchangeProxyForUse = null;
		synchronized (lastExchangeProsyUsedNum) {
			nextExchangeProxyForUse = exchangePoll.get(lastExchangeProsyUsedNum);
			if (lastExchangeProsyUsedNum < exchangePoll.size() - 1) {
				lastExchangeProsyUsedNum++;
			} else {
				lastExchangeProsyUsedNum = 0;
			}
		}
		return nextExchangeProxyForUse;
	}

	private Exchange getExchangeProxy(String proxyHost, int proxyPort, String exchangeClassName) {
		Exchange exchangeViaProxy = ExchangeFactory.INSTANCE.createExchange(exchangeClassName);
		ExchangeSpecification exchangeSpec = exchangeViaProxy.getDefaultExchangeSpecification();
		exchangeSpec.setProxyHost(proxyHost);
		exchangeSpec.setProxyPort(proxyPort);
		exchangeViaProxy.applySpecification(exchangeSpec);
		return exchangeViaProxy;
	}
}
