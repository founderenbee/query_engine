package com.inmobi.corp.clarity.resources.audit;

import org.glassfish.jersey.server.monitoring.MonitoringStatistics;
import org.glassfish.jersey.server.monitoring.TimeWindowStatistics;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/ServiceStats")
public class ServiceStatistics {
    @Inject
    private
    Provider<MonitoringStatistics> monitoringStatisticsProvider;

    @GET
    public String getStatistics() {

        final TimeWindowStatistics timeWindowStatistics = monitoringStatisticsProvider.get()
                                                            .getResourceClassStatistics()
                                                            .get(com.inmobi.corp.clarity.resources.root.QueryBuilderService.class)
                                                            .getResourceMethodExecutionStatistics()
                                                            .getTimeWindowStatistics()
                                                            .get(0L);

        return "# of requests: " + timeWindowStatistics.getRequestCount()
                + ", Avg.request processing time [ms]: " + timeWindowStatistics.getAverageDuration();
    }
}