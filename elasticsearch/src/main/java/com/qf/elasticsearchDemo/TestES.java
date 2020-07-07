/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TestES
 * Author: yanglan88
 * Date: 2020/6/19 16:34
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/19
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/19
 * @since 1.0.0
 */
package com.qf.elasticsearchDemo;

import com.qf.elasticsearchDemo.Utils.CommonUtils;
import com.qf.elasticsearchDemo.Utils.ElasticSearchUtils;
import com.qf.elasticsearchDemo.bean.Emp;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.HashMap;
import java.util.Map;

public class TestES {

    private final static String INDEX = "nihao";
    private final static String TYPE = "emp";


    public static void main(String[] args) {
        TestES es = new TestES();
        es.createIndex();
//        es.createIndexForMap();
//        es.createIndexForBean();
//        es.createIndexForContentBuilder();
//        es.updateIndex();
//        es.deleteIndex();
//        es.searchContext();
    }


    public void createIndex(){
        TransportClient client = ElasticSearchUtils.getClient();
        String json = "{\"name\":\"nimasile\",\"age\":250}";

        IndexResponse response = client.prepareIndex(INDEX, TYPE, "1")
                //TODO 创建索引
                .setSource(json, XContentType.JSON)
                .get();

        System.out.println(response.getVersion());
    }

    public void createIndexForMap(){
        TransportClient client = ElasticSearchUtils.getClient();

        Map<String,Object> map = new HashMap<>();
        map.put("name","shiyanglan");
        map.put("age",13);

        IndexResponse response = client.prepareIndex(INDEX, TYPE, "4")
                //TODO 创建索引
                .setSource(map, XContentType.JSON)
                .get();

        System.out.println(response.getVersion());
    }

    public void createIndexForBean(){
        TransportClient client = ElasticSearchUtils.getClient();

        Emp emp = new Emp("beanDemo2",43);
        Map<String, Object> map = CommonUtils.bean2Map(emp);

        IndexResponse response = client.prepareIndex(INDEX, TYPE, "7")
                //TODO 创建索引
                .setSource(map, XContentType.JSON)
                .get();

        System.out.println(response.getVersion());
    }

    public void createIndexForContentBuilder (){
        try {

            TransportClient client = ElasticSearchUtils.getClient();

            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject()
                    .field("name","hhhhh")
                    .field("age",0.9)
                    .endObject();

            IndexResponse response = client.prepareIndex(INDEX, TYPE, "6")
                    //TODO 创建索引
                    .setSource(builder)
                    .get();

            System.out.println(response.getVersion());

        }catch(Exception e){
            e.printStackTrace();
        }
    }





    public void updateIndex(){
        TransportClient client = ElasticSearchUtils.getClient();
        String json = "{\"name\":\"baby\",\"age\":\"1\"}";

        UpdateResponse updateResponse = client.prepareUpdate(INDEX, TYPE, "3")
                .setDoc(json, XContentType.JSON)
                .get();
        System.out.println(updateResponse.getVersion());
    }

    public void deleteIndex(){
        TransportClient client = ElasticSearchUtils.getClient();

        DeleteResponse deleteResponse = client.prepareDelete(INDEX, TYPE, "3")
                .get();

        System.out.println(deleteResponse.getVersion());
    }

    public void buikIndex(){
        try {
            TransportClient client = ElasticSearchUtils.getClient();
            String json = "{\"name\":\"baby\",\"age\":\"1\"}";

            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject()
                    .field("name", "nihao")
                    .field("age", "12")
                    .endObject();

            BulkResponse bulkItemResponses = client.prepareBulk()
                    .add(client.prepareIndex(INDEX, TYPE, "3").setSource(builder))
                    .add(client.prepareUpdate(INDEX, TYPE, "2").setDoc(json, XContentType.JSON))
                    .add(client.prepareDelete(INDEX, TYPE, "1"))
                    .get();

            for (BulkItemResponse respons : bulkItemResponses) {
                String id = respons.getId();
                long version = respons.getVersion();
                System.out.println(version);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    private static final String DEFAULT_INDEX = "bigdata";

    public void searchContext(String ... indexes) {

        if (indexes.length == 0){
            indexes = new String[1];
            indexes[0] = DEFAULT_INDEX;
        }

        TransportClient client = ElasticSearchUtils.getClient();

        SearchResponse searchResponse = client.prepareSearch(indexes)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchPhraseQuery("name", "baby"))
                .get();

        //命中率
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.totalHits;
        float maxScore = hits.getMaxScore();
        System.out.println("totalHits:" + totalHits + "\t" +"maxScore:" + maxScore);

        SearchHit[] hitsHits = hits.getHits();

        for (SearchHit hitsHit : hitsHits) {
            String index = hitsHit.getIndex();
            String type = hitsHit.getType();
            String id = hitsHit.getId();
            float score = hitsHit.getScore();
            String source = hitsHit.getSourceAsString();
            System.out.println(index + type + id + score + source);
        }
    }
}
