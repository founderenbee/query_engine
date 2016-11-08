package com.inmobi.corp.clarity.resources.root;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.inmobi.corp.clarity.dao.QBMetaDao;
import com.inmobi.corp.clarity.meta.MetaQuery;
import com.inmobi.corp.clarity.meta.QueryResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;

@Path("/SQL/")
@Service
@Scope("request")
public class QueryBuilderService {

    @Autowired(required = true)
    private QBMetaDao metaDao;


    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/meta/{datasource:([^/]+?)?}")
    public Response getMetaData(@PathParam("datasource") String datasource){
        return Response.ok().entity(metaDao.getAvailableFieldsList(datasource)).build();
    }

    /**
     * /MetaQuery/generate?meta="meta_query" generates and returns respective SQL Dialect Query.
     * @param metaQueryJSON The input meta query JSON.
     * @param prettyFormat The input meta query JSON.
     */
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/generate")
    public Response generate(final @DefaultValue("false") @QueryParam("format") boolean prettyFormat, final @QueryParam("meta_query") String metaQueryJSON)
    {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            mapper.setDateFormat(format);
            mapper.configure(SerializationFeature.WRAP_EXCEPTIONS, true);
            MetaQuery metaQuery = mapper.readValue(metaQueryJSON, MetaQuery.class);
            QueryResponse queryResponse = metaQuery.fetchFinalQuery(prettyFormat);
            StringWriter stringOutputQuery = new StringWriter();
            mapper.writeValue(stringOutputQuery, queryResponse );
            return Response.ok().entity(stringOutputQuery.toString()).build();
        } catch(IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/validate")
    public Response validate(final @DefaultValue("") @QueryParam("meta_query") String metaQueryJSON)
    {
        String sampleMetaQueryJSON = "{\n" +
                "   \"dateMode\":\"EVENT\",\n" +
                "   \"fromDate\":\"2016-01-05 00:00\",\n" +
                "   \"toDate\":\"2016-01-06 00:00\",\n" +
                "   \"attributes\":[\n" +
                "      {\n" +
                "         \"column\":\"ADG_ADGROUP_NAME\",\n" +
                "         \"alias\":\"\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"column\":\"ADV_ACCOUNT_GUID\",\n" +
                "         \"alias\":\"\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"column\":\"ADV_NAME\",\n" +
                "         \"alias\":\"\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"column\":\"ADV_COUNTRY_ID\",\n" +
                "         \"alias\":\"\"\n" +
                "      }\n" +
                "   ],\n" +
                "   \"measures\":[\n" +
                "      {\n" +
                "         \"column\":\"TOTAL_BURN\",\n" +
                "         \"alias\":\"\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"column\":\"TOTAL_CLICKS\",\n" +
                "         \"alias\":\"\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"column\":\"DOWNLOADS\",\n" +
                "         \"alias\":\"\"\n" +
                "      }\n" +
                "   ],\n" +
                "   \"filters\":[\n" +
                "      {\n" +
                "         \"type\":\"simple\",\n" +
                "         \"obj\":{\n" +
                "            \"filterColumn\":\"ADV_NAME\", \n" +
                "            \"filterValue\":[ \"Test_Account_KB\",\"SuningiOS\",\"Fetch%20Media%20USA\"],\n" +
                "            \"operator\":\"IN\"\n" +
                "         }\n" +
                "      },\n" +
                "      {\n" +
                "         \"type\":\"simple\",\n" +
                "         \"obj\":{\n" +
                "            \"filterColumn\":\"OS_ID\",\n" +
                "            \"filterValue\":[1,5],\n" +
                "            \"operator\":\"IN\"\n" +
                "         }\n" +
                "      }\n" +
                "   ],\n" +
                "   \"orderBy\":[\n" +
                "      {\n" +
                "         \"columnName\":\"TOTAL_BURN\",\n" +
                "         \"orderBy\":\"DESC\"\n" +
                "      }\n" +
                "   ],\n" +
                "   \"limit\":0,\n" +
                "   \"offset\":0,\n" +
                "   \"dialect\":\"VOLTDB\",\n" +
                "   \"queryType\":\"META_JSON\"\n" +
                "}";

        sampleMetaQueryJSON = ( metaQueryJSON == null || metaQueryJSON.equals("")) ? sampleMetaQueryJSON : metaQueryJSON;

        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            mapper.setDateFormat(format);
            mapper.configure(SerializationFeature.WRAP_EXCEPTIONS, true);
            MetaQuery metaQuery = mapper.readValue(sampleMetaQueryJSON, MetaQuery.class);
            if(metaQuery.validate()) {
                StringWriter stringMetaQuery = new StringWriter();
                mapper.writeValue(stringMetaQuery, metaQuery);
                return Response.ok().entity(stringMetaQuery.toString()).build();
            }
            else {
                return Response.ok().entity("Invalid JSON : " + sampleMetaQueryJSON ).build();
            }
        } catch(IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

}