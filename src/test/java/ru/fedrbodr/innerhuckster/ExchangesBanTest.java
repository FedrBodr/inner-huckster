package ru.fedrbodr.innerhuckster;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.Bittrex;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.hitbtc.v2.HitbtcExchange;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * For testing set downloadCount > 0
 */
@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
public class ExchangesBanTest {
	@Value("#{'${proxy.list}'.split(',')}")
	private List<String> proxyList;
	private int downloadCount = 0;

	@Test
	public void testBittrexCurrencyWays() {
		Exchange exchange = ExchangeFactory.INSTANCE.createExchange(BittrexExchange.class.getName());

		/*int threadCount = 30;
		ExecutorService executor;
		List<Exchange> exchangePoll = new ArrayList<>();
		exchangePoll.add(exchange);
		for (String prosyHostIp : proxyList) {
			String[] host0Ip1 = prosyHostIp.split(":");
			exchangePoll.add(getExchangeProxy(host0Ip1[0], Integer.parseInt(host0Ip1[1])));
		}*/

		List<Currency> startCurrencyes = Arrays.asList(Currency.USDT, Currency.BTC, Currency.ETH);
		List<CurrencyPair> exchangeSymbols = exchange.getExchangeSymbols();
		Date allStarDate = new Date();
		/* iterate over al symbols and try to find some vay to other payrs - maybe profit?*/
		for (Currency startCurrency : startCurrencyes) {
			log.info("Start working with main start currency {}", startCurrency);
			List<CurrencyPair> firstStepSymbols = exchangeSymbols.stream().filter(
					p -> p.counter.getCurrencyCode().equals(startCurrency.getCurrencyCode())
			).collect(Collectors.toList());
			if (CollectionUtils.isNotEmpty(firstStepSymbols)) {
				for (CurrencyPair firstStepSymbol : firstStepSymbols) {
					log.info("START Try found profit inner way to trade with FIRST step symbol {}", firstStepSymbol);
					String baseFirstStep = firstStepSymbol.base.getCurrencyCode();
					String counterFirstStep = firstStepSymbol.counter.getCurrencyCode();

					List<CurrencyPair> seccondStepSymbols = exchangeSymbols.stream().filter(p -> p.base.getCurrencyCode().equals(baseFirstStep)).collect(Collectors.toList());
					if (CollectionUtils.isNotEmpty(seccondStepSymbols)) {
						for (CurrencyPair seccondStepSymbol : seccondStepSymbols) {
							if (seccondStepSymbol.base.getCurrencyCode().equals(firstStepSymbol.base.getCurrencyCode()) &&
									seccondStepSymbol.base.getCurrencyCode().equals(firstStepSymbol.base.getCurrencyCode())) {
								log.info("Founded symbol for inner way SECCOND symbol {}", seccondStepSymbol);
								String baseSeccondStep = seccondStepSymbol.base.getCurrencyCode();
								String counterSeccondStep = seccondStepSymbol.counter.getCurrencyCode();

								List<CurrencyPair> basethirdStepSymbols = exchangeSymbols.stream().filter(p ->
												p.base.getCurrencyCode().equals(counterSeccondStep) && p.counter.getCurrencyCode().equals(startCurrency)
								).collect(Collectors.toList());

								for (CurrencyPair thirdStepSymbol : basethirdStepSymbols) {
									log.info("FOUNDED 3 WAY from start currency {} : {} + {} + {}",
											startCurrency, firstStepSymbol, seccondStepSymbol, thirdStepSymbol);
								}
							}
						}
					}
				}
			}
		}
		log.info("Get all currencies end, execution time in seconds: {}", (new Date().getTime() - allStarDate.getTime()) / 1000);

//		int marketDataServiceId = exchangePoll.size()-1;
//		log.info("threadCount {}", threadCount);
//		for(int i = downloadCount; i>0; i--) {
//			executor = Executors.newFixedThreadPool(threadCount);
//			Date oneLoadStarDate = new Date();
//			for (CurrencyPair exchangeSymbol : exchangeSymbols) {
//				MarketDataService marketDataService = exchangePoll.get(marketDataServiceId).getMarketDataService();
//				Exchange exchange1 = exchangePoll.get(marketDataServiceId);
//				if (marketDataServiceId > 0) {
//					marketDataServiceId--;
//				} else {
//					marketDataServiceId = exchangePoll.size() - 1;
//				}
//
//				Callable<Void> tCallable = () -> {
//					Date starDate = new Date();
//					try {
//						marketDataService.getOrderBook(exchangeSymbol, 100);
//						if(new Date().getTime() - starDate.getTime() > 1000){
//							log.info("Slow get orderBook for symbol " + exchangeSymbol + " ProxyHost " +exchange1.getExchangeSpecification().getProxyHost() +
//									" Execution time is "+(new Date().getTime() - starDate.getTime()));
//						}
//					} catch (Exception e) {
//						//log.error(e.getMessage() + " " + exchangeSymbol + " ProxyHost " +exchange1.getExchangeSpecification().getProxyHost(), e);
//						log.error("Invalid market execution time: {}", new Date().getTime() - starDate.getTime());
//					}
//
//					return null;
//				};
//				executor.submit(tCallable);
//			}
//
//			executor.shutdown();
//			try {
//				executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
//			} catch (InterruptedException e) {
//				log.error(e.getMessage(), e);
//			}
//			log.info("{} one load all currencies order bok end, execution time in seconds: {}", exchangeClassName, (new Date().getTime() - oneLoadStarDate.getTime()) / 1000);
//		}
//		log.info("{} get all currencies order bok end, execution time in seconds: {}", exchangeClassName, (new Date().getTime() - allStarDate.getTime()) / 1000);
	}

	@Test
	public void testBittrexBan() {
		getAllCurrencyMarketsIter(BittrexExchange.class.getName());
	}

	@Test
	public void testHitBtcBan() {
		getAllCurrencyMarketsIter(HitbtcExchange.class.getName());
	}

	private void getAllCurrencyMarketsIter(String exchangeClassName) {
		Exchange exchange = ExchangeFactory.INSTANCE.createExchange(exchangeClassName);
		Date allStarDate = new Date();
		List<CurrencyPair> exchangeSymbols = exchange.getExchangeSymbols();
		int threadCount = 30;
		ExecutorService executor;
		List<Exchange> exchangePoll = new ArrayList<>();
		exchangePoll.add(exchange);
		for (String prosyHostIp : proxyList) {
			String[] host0Ip1 = prosyHostIp.split(":");
			exchangePoll.add(getExchangeProxy(host0Ip1[0], Integer.parseInt(host0Ip1[1])));
		}

		int marketDataServiceId = exchangePoll.size() - 1;
		log.info("threadCount {}", threadCount);
		for (int i = downloadCount; i > 0; i--) {
			executor = Executors.newFixedThreadPool(threadCount);
			Date oneLoadStarDate = new Date();
			for (CurrencyPair exchangeSymbol : exchangeSymbols) {
				MarketDataService marketDataService = exchangePoll.get(marketDataServiceId).getMarketDataService();
				Exchange exchange1 = exchangePoll.get(marketDataServiceId);
				if (marketDataServiceId > 0) {
					marketDataServiceId--;
				} else {
					marketDataServiceId = exchangePoll.size() - 1;
				}

				Callable<Void> tCallable = () -> {
					Date starDate = new Date();
					try {
						marketDataService.getOrderBook(exchangeSymbol, 100);
						if (new Date().getTime() - starDate.getTime() > 1000) {
							log.info("Slow get orderBook for symbol " + exchangeSymbol + " ProxyHost " + exchange1.getExchangeSpecification().getProxyHost() +
									" Execution time is " + (new Date().getTime() - starDate.getTime()));
						}
					} catch (Exception e) {
						//log.error(e.getMessage() + " " + exchangeSymbol + " ProxyHost " +exchange1.getExchangeSpecification().getProxyHost(), e);
						log.error("Invalid market execution time: {}", new Date().getTime() - starDate.getTime());
					}

					return null;
				};
				executor.submit(tCallable);
			}

			executor.shutdown();
			try {
				executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
			log.info("{} one load all currencies order bok end, execution time in seconds: {}", exchangeClassName, (new Date().getTime() - oneLoadStarDate.getTime()) / 1000);
		}
		log.info("{} get all currencies order bok end, execution time in seconds: {}", exchangeClassName, (new Date().getTime() - allStarDate.getTime()) / 1000);
	}

	private Exchange getExchangeProxy(String proxyHost, int proxyPort) {
		Exchange exchangeViaProxy = ExchangeFactory.INSTANCE.createExchange(BittrexExchange.class.getName());
		ExchangeSpecification exchangeSpec = exchangeViaProxy.getDefaultExchangeSpecification();
		exchangeSpec.setProxyHost(proxyHost);
		exchangeSpec.setProxyPort(proxyPort);
		exchangeViaProxy.applySpecification(exchangeSpec);
		return exchangeViaProxy;
	}

}
