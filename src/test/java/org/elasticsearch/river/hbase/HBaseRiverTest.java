package org.elasticsearch.river.hbase;

import mockit.Mock;
import mockit.MockUp;
import org.elasticsearch.client.Client;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HBaseRiverTest {
  @Test
  public void testNormalizeField() {
    new MockUp<AbstractRiverComponent>() {
      @Mock
      void $init(final RiverName riverName, final RiverSettings settings) {
      }
    };
    new MockUp<HBaseRiver>() {
      @Mock
      void $init(final RiverName riverName, final RiverSettings settings, final Client esClient) {
      }

      @Mock
      boolean isNormalizeFields() {
        return true;
      }

      @Mock
      String getColumnSeparator() {
        return "::";
      }
    };

    final HBaseRiver river = new HBaseRiver(null, null, null);

    Assert.assertEquals(river.normalizeField(""), "");
    Assert.assertEquals(river.normalizeField(" "), "");
    Assert.assertEquals(river.normalizeField("a"), "a");
    Assert.assertEquals(river.normalizeField("A"), "a");
    Assert.assertEquals(river.normalizeField("Aa"), "aa");
    Assert.assertEquals(river.normalizeField("a-b"), "a-b");
    Assert.assertEquals(river.normalizeField("a_b"), "a_b");
    Assert.assertEquals(river.normalizeField("90aS"), "90as");
    Assert.assertEquals(river.normalizeField("&*($@#!ui^&$(#\"8ui"), "ui8ui");
    Assert.assertEquals(river.normalizeField("bl%^&*ah::blubb"), "blah::blubb");
    Assert.assertEquals(river.normalizeField(null), null);
  }
}
