package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.WorkloadException;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGraphWorkload {

  @Test
  public void doInsertFailing() throws WorkloadException {
    DB db = mock(DB.class);
    Status status = mock(Status.class);

    when(status.isOk()).thenReturn(false);
    when(db.insert(anyString(), anyString(), anyMap())).thenReturn(status);

    GraphWorkload graphWorkload = new GraphWorkload();
    graphWorkload.init(new Properties());

    Assert.assertFalse(graphWorkload.doInsert(db, new Object()));
  }

  @Test
  public void doInsertSuccessful() throws WorkloadException {
    DB db = mock(DB.class);
    Status status = mock(Status.class);

    when(status.isOk()).thenReturn(true);
    when(db.insert(anyString(), anyString(), anyMap())).thenReturn(status);

    GraphWorkload graphWorkload = new GraphWorkload();
    graphWorkload.init(new Properties());

    Assert.assertTrue(graphWorkload.doInsert(db, new Object()));
  }
}
