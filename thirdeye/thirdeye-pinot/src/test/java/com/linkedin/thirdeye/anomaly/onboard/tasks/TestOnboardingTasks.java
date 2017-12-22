package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTaskContext;
import com.linkedin.thirdeye.anomaly.onboard.OnboardingTaskTestUtils;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestOnboardingTasks {
  private DAOTestBase daoTestBase;
  private DetectionOnboardTaskContext context;
  private DatasetConfigManager datasetConfigDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private JobManager jobDAO;

  @BeforeClass
  public void beforeClass(){
    daoTestBase = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
    jobDAO = daoRegistry.getJobDAO();
    context = OnboardingTaskTestUtils.getDetectionTaskContext();
    initDataset();
  }

  public void initDataset(){
    // Prepare for data
    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setDataset(OnboardingTaskTestUtils.TEST_COLLECTION);
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.DAYS);
    datasetConfig.setTimeDuration(1);
    datasetConfig.setTimeFormat("SIMPLE_DATE_FORMAT:yyyyMMdd");
    datasetConfig.setTimezone("US/Pacific");
    datasetConfigDAO.save(datasetConfig);
    Assert.assertNotNull(datasetConfigDAO.findByDataset(OnboardingTaskTestUtils.TEST_COLLECTION));
  }
  @AfterClass(alwaysRun = true)
  public void afterClass(){
    daoTestBase.cleanup();
  }

  @Test
  public void testOnboardingTasks() throws Exception{
    DetectionOnboardTask task = new DataPreparationOnboardingTask();
    task.setTaskContext(context);
    task.run();

    DetectionOnboardExecutionContext executionContext = context.getExecutionContext();
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.FUNCTION_FACTORY));
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY));
    Assert.assertNotNull(executionContext.getExecutionResult(DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY));

    task = new FunctionCreationOnboardingTask();
    task.setTaskContext(context);
    task.run();

    Assert.assertEquals(1, anomalyFunctionDAO.findAll().size());
    Assert.assertEquals(1, alertConfigDAO.findAll().size());

    FunctionReplayOnboardingTask replayTask = new FunctionReplayOnboardingTask();
    replayTask.setTaskContext(context);
    replayTask.initDetectionJob();

    Assert.assertEquals(1, jobDAO.findAll().size());

    task = new AlertFilterAutoTuneOnboardingTask();
    task.setTaskContext(context);
    task.run();
  }
}