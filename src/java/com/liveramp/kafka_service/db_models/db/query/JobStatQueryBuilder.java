package com.liveramp.kafka_service.db_models.db.query;

import java.util.Set;

import com.rapleaf.jack.queries.AbstractQueryBuilder;
import com.rapleaf.jack.queries.FieldSelector;
import com.rapleaf.jack.queries.where_operators.IWhereOperator;
import com.rapleaf.jack.queries.where_operators.JackMatchers;
import com.rapleaf.jack.queries.WhereConstraint;
import com.rapleaf.jack.queries.QueryOrder;
import com.rapleaf.jack.queries.OrderCriterion;
import com.rapleaf.jack.queries.LimitCriterion;
import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
import com.liveramp.kafka_service.db_models.db.models.JobStat;


public class JobStatQueryBuilder extends AbstractQueryBuilder<JobStat> {

  public JobStatQueryBuilder(IJobStatPersistence caller) {
    super(caller);
  }

  public JobStatQueryBuilder select(JobStat._Fields... fields) {
    for (JobStat._Fields field : fields){
      addSelectedField(new FieldSelector(field));
    }
    return this;
  }

  public JobStatQueryBuilder selectAgg(FieldSelector... aggregatedFields) {
    addSelectedFields(aggregatedFields);
    return this;
  }

  public JobStatQueryBuilder id(Long value) {
    addId(value);
    return this;
  }

  public JobStatQueryBuilder idIn(Set<Long> values) {
    addIds(values);
    return this;
  }

  public JobStatQueryBuilder limit(int offset, int nResults) {
    setLimit(new LimitCriterion(offset, nResults));
    return this;
  }

  public JobStatQueryBuilder limit(int nResults) {
    setLimit(new LimitCriterion(nResults));
    return this;
  }

  public JobStatQueryBuilder groupBy(JobStat._Fields... fields) {
    addGroupByFields(fields);
    return this;
  }

  public JobStatQueryBuilder order() {
    this.addOrder(new OrderCriterion(QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder order(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(queryOrder));
    return this;
  }
  
  public JobStatQueryBuilder orderById() {
    this.addOrder(new OrderCriterion(QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder orderById(QueryOrder queryOrder) {    
    this.addOrder(new OrderCriterion(queryOrder));
    return this;
  }

  public JobStatQueryBuilder jobId(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.job_id, JackMatchers.equalTo(value)));
    return this;
  }

  public JobStatQueryBuilder whereJobId(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.job_id, operator));
    return this;
  }
  
  public JobStatQueryBuilder orderByJobId() {
    this.addOrder(new OrderCriterion(JobStat._Fields.job_id, QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder orderByJobId(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(JobStat._Fields.job_id, queryOrder));
    return this;
  }

  public JobStatQueryBuilder countError(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.count_error, JackMatchers.equalTo(value)));
    return this;
  }

  public JobStatQueryBuilder whereCountError(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.count_error, operator));
    return this;
  }
  
  public JobStatQueryBuilder orderByCountError() {
    this.addOrder(new OrderCriterion(JobStat._Fields.count_error, QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder orderByCountError(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(JobStat._Fields.count_error, queryOrder));
    return this;
  }

  public JobStatQueryBuilder countActualTotal(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.count_actual_total, JackMatchers.equalTo(value)));
    return this;
  }

  public JobStatQueryBuilder whereCountActualTotal(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.count_actual_total, operator));
    return this;
  }
  
  public JobStatQueryBuilder orderByCountActualTotal() {
    this.addOrder(new OrderCriterion(JobStat._Fields.count_actual_total, QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder orderByCountActualTotal(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(JobStat._Fields.count_actual_total, queryOrder));
    return this;
  }

  public JobStatQueryBuilder countExpectedTotal(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.count_expected_total, JackMatchers.equalTo(value)));
    return this;
  }

  public JobStatQueryBuilder whereCountExpectedTotal(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.count_expected_total, operator));
    return this;
  }
  
  public JobStatQueryBuilder orderByCountExpectedTotal() {
    this.addOrder(new OrderCriterion(JobStat._Fields.count_expected_total, QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder orderByCountExpectedTotal(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(JobStat._Fields.count_expected_total, queryOrder));
    return this;
  }

  public JobStatQueryBuilder createdAt(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.created_at, JackMatchers.equalTo(value)));
    return this;
  }

  public JobStatQueryBuilder whereCreatedAt(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.created_at, operator));
    return this;
  }
  
  public JobStatQueryBuilder orderByCreatedAt() {
    this.addOrder(new OrderCriterion(JobStat._Fields.created_at, QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder orderByCreatedAt(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(JobStat._Fields.created_at, queryOrder));
    return this;
  }

  public JobStatQueryBuilder updatedAt(Long value) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.updated_at, JackMatchers.equalTo(value)));
    return this;
  }

  public JobStatQueryBuilder whereUpdatedAt(IWhereOperator<Long> operator) {
    addWhereConstraint(new WhereConstraint<Long>(JobStat._Fields.updated_at, operator));
    return this;
  }
  
  public JobStatQueryBuilder orderByUpdatedAt() {
    this.addOrder(new OrderCriterion(JobStat._Fields.updated_at, QueryOrder.ASC));
    return this;
  }
  
  public JobStatQueryBuilder orderByUpdatedAt(QueryOrder queryOrder) {
    this.addOrder(new OrderCriterion(JobStat._Fields.updated_at, queryOrder));
    return this;
  }
}
