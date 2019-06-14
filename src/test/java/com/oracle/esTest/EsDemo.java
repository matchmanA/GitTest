package com.oracle.esTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.document.DateTools.Resolution;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

public class EsDemo {
	/**
	 * @Title test1
	 * @Description 查询文档
	 * @throws UnknownHostException
	 */
	@Test
	public void test1() throws UnknownHostException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		// 数据查询
		GetResponse response = client.prepareGet("lib", "user", "1").execute().actionGet();
		// 得到查询的数据
		System.out.println(response.getSourceAsString());
		client.close();

	}

	/**
	 * @Title test2
	 * @Description 添加文档
	 * @throws IOException
	 */
	@Test
	public void test2() throws IOException {
		// 添加此类型文档
		/**
		 * "{" + "\"id\":\"1\"," + "\"title\":\"Java设计模式之装饰模式\"," +
		 * "\"content\":\"在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。\"," +
		 * "\"postdate\":\"2018-05-20 14:38:00\"," +
		 * "\"url\":\"csdn.net/79239072\"" + "}" 因为包含汉字要使用中文分词器
		 */
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		// 数据查询
		XContentBuilder xcb = XContentFactory.jsonBuilder().
				startObject()
				.field("id", "1")
				.field("title", "Java设计模式之装饰模式")
				.field("content", "在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。")
				.field("postdate", "2018-05-20")//添加时带时分秒报错
				.field("url", "csdn.net/79239072")
				.endObject();
		IndexResponse response = client.prepareIndex("index", "blog", "10").setSource(xcb).get();
		System.out.println(response.status());
		client.close();

	}

	/**
	 * @Title test3
	 * @Description 删除文档
	 * @throws IOException
	 */
	@Test
	public void test3() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		DeleteResponse response = client.prepareDelete("index", "blog", "10").get();
		System.out.println(response.status());
		client.close();
	}
	
	/**
	 * @Title test4
	 * @Description 更新文档doc方式
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test4() throws IOException, InterruptedException, ExecutionException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		UpdateRequest request = new UpdateRequest();
		request.index("index")
			   .type("blog")
			   .id("10")
			   .doc(
					   XContentFactory.jsonBuilder()
					                  .startObject()
					                  .field("title","修改后的标题")
					                  .endObject()
					   );
		UpdateResponse response = client.update(request).get();
		System.out.println(response.status());
		client.close();
	}
	/**
	 * @Title test5
	 * @Description 更新文档upSert方式
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test5() throws IOException, InterruptedException, ExecutionException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		IndexRequest request1 = new IndexRequest("index","blog","11")
							   .source(
									   XContentFactory.jsonBuilder()
									   .startObject()
									   .field("id", "2")
										.field("title", "Java设计模式之装饰模式")
										.field("content", "在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。")
										.field("postdate", "2018-05-20")//添加时带时分秒报错
										.field("url", "csdn.net/79239072")
										.endObject()
									   );
		UpdateRequest request2 = new UpdateRequest();
		request2.index("index")
			   .type("blog")
			   .id("11")
			   .doc(
					   XContentFactory.jsonBuilder()
					                  .startObject()
					                  .field("title","修改后的标题2222222")
					                  .endObject()
					   ).upsert(request1);
		UpdateResponse response = client.update(request2).get();
		System.out.println(response.status());
		client.close();
	}
	
	/**
	 * @Title test6
	 * @Description multiGet查询
	 * @throws IOException
	 */
	@Test
	public void test6() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		MultiGetResponse response = client.prepareMultiGet()
									.add("index", "blog", "10" ,"11")
									//.add("index", "user", "10" ,"11")
									.get();
		for (MultiGetItemResponse item : response) {
			GetResponse gr = item.getResponse();
			if(gr != null && gr.isExists()){
				System.out.println(gr.getSourceAsString());
			}
		}
		client.close();
	}
	
	/**
	 * @Title test7
	 * @Description bulk使用
	 * @throws IOException
	 */
	@Test
	public void test7() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		BulkRequestBuilder brb = client.prepareBulk();
		//批量添加
		brb.add(client.prepareIndex("index","blog", "12")
				.setSource(
						XContentFactory.jsonBuilder()
						.startObject()
						.field("id", "3")
						.field("title", "Java设计模式之装饰模式121212")
						.field("content", "在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。")
						.field("postdate", "2018-05-20")//添加时带时分秒报错
						.field("url", "csdn.net/79239072")
						.endObject()
						)
				);
		brb.add(client.prepareIndex("index","blog", "13")
				.setSource(
						XContentFactory.jsonBuilder()
						.startObject()
						.field("id", "4")
						.field("title", "Java设计模式之装饰模式1212124444444444444")
						.field("content", "在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。")
						.field("postdate", "2018-05-20")//添加时带时分秒报错
						.field("url", "csdn.net/79239072")
						.endObject()
						)
				);
		BulkResponse response = brb.get();
		System.out.println(response.status());
		if(response.hasFailures()){
			System.out.println("失败");
		}
	}
	
	/**
	 * @Title test8
	 * @Description 删除查询的
	 * @throws IOException
	 */
	@Test
	public void test8() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
										.newRequestBuilder(client)
										.filter(QueryBuilders.matchQuery("title", "修改后的标题"))
										.source("index")
										.get();
		Long count = response.getDeleted();
		System.out.println(count);
	}
	
	/**
	 * @Title test9
	 * @Description match_all 查询所有
	 * @throws IOException
	 */
	@Test
	public void test9() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		QueryBuilder qb = QueryBuilders.matchAllQuery();
		SearchResponse response = client.prepareSearch("index")
				                  .setQuery(qb)
				                  .setSize(3)
				                  .get();
			SearchHits shs = response.getHits();
			for (SearchHit searchHit : shs) {
				System.out.println(searchHit.getSourceAsString());
				Map<String,Object> map = searchHit.getSourceAsMap();
				for(String key : map.keySet()){
					System.out.println(map.get(key));
				}
			}
		}
	
	/**
	 * @Title test10
	 * @Description match query 查询
	 * @throws IOException
	 */
	@Test
	public void test10() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		QueryBuilder qb = QueryBuilders.matchQuery("title", "模式12");
		SearchResponse response = client.prepareSearch("index").setQuery(qb).setSize(2).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
			Map<String,Object> map = searchHit.getSourceAsMap();
			for(String key : map.keySet()){
				System.out.println(map.get(key));
			}
		}
		
		}
	/**
	 * @Title test11
	 * @Description multi match query 查询
	 * @throws IOException
	 */
	@Test
	public void test11() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		QueryBuilder qb = QueryBuilders.multiMatchQuery("设计", "title","content");
		
		SearchResponse response = client.prepareSearch("index").setQuery(qb).setSize(2).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
			Map<String,Object> map = searchHit.getSourceAsMap();
			for(String key : map.keySet()){
				System.out.println(map.get(key));
			}
		}
		
		}
	/**
	 * @Title test12
	 * @Description term 查询
	 * @throws IOException
	 */
	@Test
	public void test12() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		QueryBuilder qb = QueryBuilders.termQuery("title", "设计");
		SearchResponse response = client.prepareSearch("index").setQuery(qb).setSize(2).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
			Map<String,Object> map = searchHit.getSourceAsMap();
			for(String key : map.keySet()){
				System.out.println(map.get(key));
			}
		}
		
		}
	/**
	 * @Title test13
	 * @Description terms 查询
	 * @throws IOException
	 */
	@Test
	public void test13() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		QueryBuilder qb = QueryBuilders.termsQuery("title", "设计","12");
		SearchResponse response = client.prepareSearch("index").setQuery(qb).setSize(2).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
			Map<String,Object> map = searchHit.getSourceAsMap();
			for(String key : map.keySet()){
				System.out.println(map.get(key));
			}
		}
		
		}
	/**
	 * @Title test14
	 * @Description 多种方式查询
	 * @throws IOException
	 */
	@Test
	public void test14() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		//range 范围查询
		//QueryBuilder qb = QueryBuilders.rangeQuery("postdate").from("2015-10-11").to("2018-10-10");
		//prefix 前缀 范围查询
		//QueryBuilder qb = QueryBuilders.prefixQuery("title", "java");
		//wildcard 模糊查询
		//QueryBuilder qb = QueryBuilders.wildcardQuery("title", "*12*");
		//fuzzy 查询
		//QueryBuilder qb = QueryBuilders.fuzzyQuery("title", "装饰");
		//type 查询
		//QueryBuilder qb = QueryBuilders.typeQuery("blog");
		//id 查询
		QueryBuilder qb = QueryBuilders.idsQuery().addIds("11","13");
		
		SearchResponse response = client.prepareSearch("index").setQuery(qb).setSize(2).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
			Map<String,Object> map = searchHit.getSourceAsMap();
			for(String key : map.keySet()){
				System.out.println(map.get(key));
			}
		}
	}
	/**
	 * @Title test15
	 * @Description 聚合查询
	 * @throws IOException
	 */
	@Test
	public void test15() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		//求id中的最大值
	/*	AggregationBuilder ab = AggregationBuilders.max("maxId").field("id");
		AggregationBuilder ab = AggregationBuilders.max("maxId").field("id");//求id中的最大值
		SearchResponse response = client.prepareSearch("index").addAggregation(ab).get();
		Max max = response.getAggregations().get("maxId");
		System.out.println(max.getValue());*/
		//最小值
		/*AggregationBuilder ab = AggregationBuilders.min("minId").field("id");
		SearchResponse response = client.prepareSearch("index").addAggregation(ab).get();
		Min min = response.getAggregations().get("minId");
		System.out.println(min.getValue());*/
		//平均值
		/*AggregationBuilder ab = AggregationBuilders.avg("avgId").field("id");
		SearchResponse response = client.prepareSearch("index").addAggregation(ab).get();
		Avg avg = response.getAggregations().get("avgId");
		System.out.println(avg.getValue());*/
		//总和
		/*AggregationBuilder ab = AggregationBuilders.sum("sumId").field("id");
		SearchResponse response = client.prepareSearch("index").addAggregation(ab).get();
		Sum sum = response.getAggregations().get("sumId");
		System.out.println(sum.getValue());*/
		//基数，一个字段上有多少不同的数
		AggregationBuilder ab = AggregationBuilders.cardinality("carId").field("id");
		SearchResponse response = client.prepareSearch("index").addAggregation(ab).get();
		Cardinality card = response.getAggregations().get("carId");
		System.out.println(card.getValue());
	}
	/**
	 * @Title test16
	 * @Description queryString 查询
	 * @throws IOException
	 */
	@Test
	public void test16() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		//QueryBuilder qb = QueryBuilders.commonTermsQuery("title", "java");
		//QueryBuilder qb = QueryBuilders.queryStringQuery("+java -设计");//+表示一定含有，— 表示不是不许有
		QueryBuilder qb = QueryBuilders.simpleQueryStringQuery("+java -设计");
		SearchResponse response = client.prepareSearch("index").setQuery(qb).setSize(2).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	/**
	 * @Title test17
	 * @Description 组合查询
	 * @throws IOException
	 */
	@Test
	public void test17() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		//bool 查询
		/*QueryBuilder qb = QueryBuilders.boolQuery()
						.must(QueryBuilders.rangeQuery("id").from(1).to(5))
						.mustNot(QueryBuilders.wildcardQuery("title", "*12*"))
						.should(QueryBuilders.prefixQuery("content", "在"))
						.filter(QueryBuilders.rangeQuery("postdate").from("2018-01-01").to("2018-10-10"));*/
		//constantScore查询
		QueryBuilder qb = QueryBuilders.constantScoreQuery(QueryBuilders.wildcardQuery("title", "*12*"));
		SearchResponse response = client.prepareSearch("index").setQuery(qb).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	/**
	 * @Title test18
	 * @Description 分组聚合查询
	 * @throws IOException
	 */
	@Test
	public void test18() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		//bool 查询
		/*QueryBuilder qb = QueryBuilders.boolQuery()
						.must(QueryBuilders.rangeQuery("id").from(1).to(5))
						.mustNot(QueryBuilders.wildcardQuery("title", "*12*"))
						.should(QueryBuilders.prefixQuery("content", "在"))
						.filter(QueryBuilders.rangeQuery("postdate").from("2018-01-01").to("2018-10-10"));*/
		//constantScore查询
		QueryBuilder qb = QueryBuilders.constantScoreQuery(QueryBuilders.wildcardQuery("title", "*12*"));
		SearchResponse response = client.prepareSearch("index").setQuery(qb).get();
		SearchHits shs = response.getHits();
		for (SearchHit searchHit : shs) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	/**
	 * @Title test18
	 * @Description 
	 * @throws IOException
	 */
	@Test
	public void test19() throws IOException {
		// 指定es集群
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 连接节点
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.73.130"), 9300));
		/*AggregationBuilder ag = AggregationBuilders
	    .filter("agg", QueryBuilders.termQuery("title", "设计"));
		SearchResponse response = client.prepareSearch("index").addAggregation(ag).get();
		Filter agg = response.getAggregations().get("agg");*/
		AggregationBuilder ag = AggregationBuilders.missing("agg").field("id");
		SearchResponse response = client.prepareSearch("index").addAggregation(ag).get();
		Missing agg = response.getAggregations().get("agg");
		agg.getDocCount(); // Doc count
		Long count = agg.getDocCount(); // Doc count
		System.out.println(count);
		
	}
}