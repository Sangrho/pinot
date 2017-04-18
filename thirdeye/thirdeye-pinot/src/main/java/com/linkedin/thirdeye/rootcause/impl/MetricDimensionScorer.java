package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.diffsummary.Cube;
import com.linkedin.thirdeye.client.diffsummary.DimNameValueCostEntry;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.OLAPDataBaseClient;
import com.linkedin.thirdeye.client.diffsummary.PinotThirdEyeSummaryClient;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The MetricDimensionScorer performs contribution analysis for sets of associated metric
 * dimensions given a time range and a baseline range. It relies on ThirdEye's internal QueryCache
 * to obtain the necessary data for constructing a data cube.
 */
public class MetricDimensionScorer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricDimensionScorer.class);

  private static final String DIMENSION = "dimension";
  private static final String COST = "cost";
  private static final String VALUE = "value";

  final QueryCache cache;

  public MetricDimensionScorer(QueryCache cache) {
    this.cache = cache;
  }

  /**
   * Perform contribution analysis on associated MetricDimension entities given a time range
   * and baseline range.
   *
   * @param entities MetricDimensionEntities of same metric and dataset
   * @param current current time range
   * @param baseline baseline time range
   * @return MetricDimensionEntities with score updated according to contribution
   * @throws Exception if data cannot be fetched or data is invalid
   */
  Collection<MetricDimensionEntity> score(Collection<MetricDimensionEntity> entities, TimeRangeEntity current, BaselineEntity baseline) throws Exception {
    if(entities.isEmpty())
      return Collections.emptyList();

    // ensure same base dataset and metric
    Iterator<MetricDimensionEntity> it = entities.iterator();
    MetricDimensionEntity first = it.next();
    MetricConfigDTO metric = first.getMetric();
    DatasetConfigDTO dataset = first.getDataset();

    while(it.hasNext()) {
      MetricDimensionEntity e = it.next();
      if(!metric.equals(e.getMetric()))
        throw new IllegalArgumentException("entities must derive from same metric");
      if(!dataset.equals(e.getDataset()))
        throw new IllegalArgumentException("entities must derive from same dataset");
    }

    // build data cube
    OLAPDataBaseClient olapClient = getOlapDataBaseClient(current, baseline, metric, dataset);
    Dimensions dimensions = new Dimensions(dataset.getDimensions());
    int topDimensions = dataset.getDimensions().size();

    Cube cube = new Cube();
    cube.buildWithAutoDimensionOrder(olapClient, dimensions, topDimensions, Collections.<List<String>>emptyList());

    // group by dimension
    DataFrame df = toNormalizedDataFrame(cube.getCostSet());

    // map dimension to MetricDimension
    Map<String, MetricDimensionEntity> mdMap = new HashMap<>();
    for(MetricDimensionEntity e : entities) {
      mdMap.put(e.getDimension(), e);
    }

    List<MetricDimensionEntity> scores = new ArrayList<>();
    for(int i=0; i<df.size(); i++) {
      String urn = df.getString(DIMENSION, i);
      MetricDimensionEntity e = mdMap.get(urn);
      if(e == null) {
        LOG.warn("Could not resolve MetricDimensionEntity '{}'. Skipping.", urn);
        continue;
      }

      // final score is dimension_contribution * base_metric_score
      MetricDimensionEntity n = e.withScore(df.getDouble(COST, i) * e.getScore());
      scores.add(n);
    }

    return scores;
  }

  private OLAPDataBaseClient getOlapDataBaseClient(TimeRangeEntity current, BaselineEntity baseline, MetricConfigDTO metric, DatasetConfigDTO dataset) throws Exception {
    final String timezone = "UTC";
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric.getName(), MetricAggFunction.SUM, dataset.getDataset());

    OLAPDataBaseClient olapClient = new PinotThirdEyeSummaryClient(cache);
    olapClient.setCollection(dataset.getDataset());
    olapClient.setMetricExpression(metricExpressions.get(0));
    olapClient.setCurrentStartInclusive(new DateTime(current.getStart(), DateTimeZone.forID(timezone)));
    olapClient.setCurrentEndExclusive(new DateTime(current.getEnd(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineStartInclusive(new DateTime(baseline.getStart(), DateTimeZone.forID(timezone)));
    olapClient.setBaselineEndExclusive(new DateTime(baseline.getEnd(), DateTimeZone.forID(timezone)));
    return olapClient;
  }

  private static DataFrame toNormalizedDataFrame(Collection<DimNameValueCostEntry> costs) {
    String[] dim = new String[costs.size()];
    String[] value = new String[costs.size()];
    double[] cost = new double[costs.size()];
    int i = 0;
    for(DimNameValueCostEntry e : costs) {
      dim[i] = e.getDimName();
      value[i] = e.getDimValue();
      cost[i] = e.getCost();
      i++;
    }

    DataFrame df = new DataFrame();
    df.addSeries(DIMENSION, dim);
    df.addSeries(COST, cost);
    df.addSeries(VALUE, value);

    DataFrame agg = df.groupBy(DIMENSION).aggregate(COST, DoubleSeries.SUM);
    DoubleSeries s = agg.getDoubles(COST);
    agg.addSeries(COST, s.divide(s.sum()));

    return agg;
  }
}