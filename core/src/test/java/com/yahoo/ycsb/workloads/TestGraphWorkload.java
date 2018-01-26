package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import junit.framework.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGraphWorkload {

  @Test
  public void doInsertFailing() {
    DB db = mock(DB.class);
    Status status = mock(Status.class);

    when(status.isOk()).thenReturn(false);
    when(db.insert(anyString(), anyString(), anyMap())).thenReturn(status);

    Assert.assertFalse(new GraphWorkload().doInsert(db, new Object()));
  }

  @Test
  public void doInsertSuccessful() {
    DB db = mock(DB.class);
    Status status = mock(Status.class);

    when(status.isOk()).thenReturn(true);
    when(db.insert(anyString(), anyString(), anyMap())).thenReturn(status);

    Assert.assertTrue(new GraphWorkload().doInsert(db, new Object()));
  }
}
