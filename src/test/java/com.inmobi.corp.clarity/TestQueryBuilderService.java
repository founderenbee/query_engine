package com.inmobi.corp.clarity;

import com.inmobi.corp.clarity.resources.root.QueryBuilderService;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.uri.UriComponent;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.util.UriUtils;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;


public class TestQueryBuilderService extends JerseyTest {

    WebTarget target = null;

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        return new ResourceConfig(QueryBuilderService.class);
    }

    @Before
    public void before_test(){
        target = target();
    }

    @Test
    public void test() {
        String meta_json = "{\"dateMode\":\"EVENT\",\"fromDate\":\"2016-09-28 00:00\",\"toDate\":\"2016-09-28 00:00\",\"attributes\":[{\"column\":\"event_full_date\",\"alias\":\"\"},{\"column\":\"advertiser_account_guid\",\"alias\":\"\"},{\"column\":\"advertiser_name\",\"alias\":\"\"}],\"measures\":[{\"column\":\"total_burn\",\"alias\":\"\"}],\"orderBy\":[],\"limit\":1000,\"offset\":0,\"dialect\":\"VERTICA\",\"queryType\":\"META_JSON\"}";

        try {
            /*
            Thread.sleep( 60 * 1000 );
            System.out.println(UriUtils.encodeQueryParam(meta_json, "UTF-8"));
            final Response response1 = target.path("/")
                    .request()
                    .get();

            System.out.println("Printing Response");
            System.out.println("####### -- " + response1.toString() + "####### -- ");
            final Response response2 = target.path("query_engine/SQL/generate/")
                                            .queryParam("meta_query", UriComponent.encode(meta_json, UriComponent.Type.QUERY_PARAM_SPACE_ENCODED))
                                            .request()
                                            .get();
            */

            // assertEquals(response.getStatus(), 200);
            // assertEquals("select adv.account_guid as "advertiser_account_guid", adv.name as "advertiser_name", evdt.full_date as "event_full_date", sum(fact.total_burn) as "total_burn" from day_agg2_demand_fact_v fact join date_dim as "evdt" on ( evdt.day_id = fact.event_day_id and evdt.date_as_int between 20160928 and 20160928 ) join ams_account as "adv" on fact.adv_account_inc_id = adv.id group by adv.account_guid, adv.name, evdt.full_date limit 1000 offset 0", response.readEntity(String.class));
        }
        catch (Exception ex){
            System.out.println("Printing Exception");
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
    }
}
