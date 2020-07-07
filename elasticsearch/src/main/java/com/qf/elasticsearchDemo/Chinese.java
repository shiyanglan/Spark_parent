/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Chinese
 * Author: yanglan88
 * Date: 2020/6/22 10:37
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 *
 * @author yanglan88
 * @create 2020/6/22
 * @since 1.0.0
 */


/**
 * @author yanglan88
 * @create 2020/6/22
 * @since 1.0.0
 */
package com.qf.elasticsearchDemo;

import com.qf.elasticsearchDemo.Utils.ElasticSearchUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class Chinese {

    private final static String INDEX = "chinese";
    private final static String TYPE = "text1";

    public static void main(String[] args) {

        TransportClient client = ElasticSearchUtils.getClient();

        SearchResponse searchResponse = client.prepareSearch(INDEX)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("content", "教练"))
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.totalHits;
        float maxScore = hits.getMaxScore();
        System.out.println("totalHits : " + totalHits + "\t" +"maxScore : " + maxScore);

        SearchHit[] hitsHits = hits.getHits();
        for (SearchHit hitsHit : hitsHits) {
            String index = hitsHit.getIndex();
            String type = hitsHit.getType();
            String id = hitsHit.getId();
            float score = hitsHit.getScore();
            String source = hitsHit.getSourceAsString();
            System.out.println(index + type + id + score + source);
        }

        /*
        totalHits : 2	maxScore : 0.1871055

        chinesetest130.1871055
        {
          "content": "教练还带领广州恒大称霸中超并首次夺得亚冠联赛"
        }

        chinesetest110.17777617
        {
          "content": "里皮是一位牌面足够大、支持率足够高的教练"
        }
         */
    }

}

