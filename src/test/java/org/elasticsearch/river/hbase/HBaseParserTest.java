package org.elasticsearch.river.hbase;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mockit;

import org.elasticsearch.client.Client;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.hbase.async.KeyValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class HBaseParserTest {
	public class ReadQualifierStructureTest {
		public String	separator;
		public boolean	normalize;

		@BeforeClass
		public void setUp() {
			new MockUp<AbstractRiverComponent>() {
				@Mock
				void $init(final RiverName riverName, final RiverSettings settings) {}
			};
			new MockUp<HBaseRiver>() {
				@Mock
				void $init(final RiverName riverName, final RiverSettings settings, final Client esClient) {}

				@Mock
				String getColumnSeparator() {
					return ReadQualifierStructureTest.this.separator;
				}

				@Mock
				boolean isNormalizeFields() {
					return ReadQualifierStructureTest.this.normalize;
				}
			};
		}

		@AfterClass
		public void tearDown() {
			Mockit.tearDownMocks();
		}

		@SuppressWarnings("unchecked")
		@Test
		public void testBase() throws Exception {
			this.separator = "::";
			this.normalize = false;

			final Map<String, Object> result = new HashMap<String, Object>();
			final HBaseParser parser = new HBaseParser(new HBaseRiver(null, null, null));
			parser.readQualifierStructure(result, "data::set1::category1", "test1");
			parser.readQualifierStructure(result, "data::set1::category2", "test2");
			parser.readQualifierStructure(result, "data::set1::category3", "test3");
			parser.readQualifierStructure(result, "data::set2::category1", "test4");
			parser.readQualifierStructure(result, "data::set2::category2", "test5");

			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set1")).get("category1"), "test1");
			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set1")).get("category2"), "test2");
			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set1")).get("category3"), "test3");
			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set2")).get("category1"), "test4");
			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set2")).get("category2"), "test5");
		}

		@Test
		public void testNullSeperator() throws Exception {
			this.separator = null;
			this.normalize = false;

			final Map<String, Object> result = new HashMap<String, Object>();
			final HBaseParser parser = new HBaseParser(new HBaseRiver(null, null, null));
			parser.readQualifierStructure(result, "data::set1::category1", "test1");
			parser.readQualifierStructure(result, "data::set1::category2", "test2");
			parser.readQualifierStructure(result, "data::set1::category3", "test3");
			parser.readQualifierStructure(result, "data::set2::category1", "test4");
			parser.readQualifierStructure(result, "data::set2::category2", "test5");

			Assert.assertEquals(result.get("data::set1::category1"), "test1");
			Assert.assertEquals(result.get("data::set1::category2"), "test2");
			Assert.assertEquals(result.get("data::set1::category3"), "test3");
			Assert.assertEquals(result.get("data::set2::category1"), "test4");
			Assert.assertEquals(result.get("data::set2::category2"), "test5");
		}

		@Test
		public void testEmptySeperator() throws Exception {
			this.separator = "";
			this.normalize = false;

			final Map<String, Object> result = new HashMap<String, Object>();
			final HBaseParser parser = new HBaseParser(new HBaseRiver(null, null, null));
			parser.readQualifierStructure(result, "data::set1::category1", "test1");
			parser.readQualifierStructure(result, "data::set1::category2", "test2");
			parser.readQualifierStructure(result, "data::set1::category3", "test3");
			parser.readQualifierStructure(result, "data::set2::category1", "test4");
			parser.readQualifierStructure(result, "data::set2::category2", "test5");

			Assert.assertEquals(result.get("data::set1::category1"), "test1");
			Assert.assertEquals(result.get("data::set1::category2"), "test2");
			Assert.assertEquals(result.get("data::set1::category3"), "test3");
			Assert.assertEquals(result.get("data::set2::category1"), "test4");
			Assert.assertEquals(result.get("data::set2::category2"), "test5");
		}

		@SuppressWarnings("unchecked")
		@Test
		public void testEmptySubQualifier() throws Exception {
			this.separator = "::";
			this.normalize = true;

			final Map<String, Object> result = new HashMap<String, Object>();
			final HBaseParser parser = new HBaseParser(new HBaseRiver(null, null, null));
			parser.readQualifierStructure(result, "data::set1::category1", "test1");
			parser.readQualifierStructure(result, "data::set1::category2", "test2");
			parser.readQualifierStructure(result, "data::set1::category3", "test3");
			parser.readQualifierStructure(result, "data::set2::category1", "test4");
			parser.readQualifierStructure(result, "data::set2::", "test5");

			System.out.println(result);

			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set1")).get("category1"), "test1");
			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set1")).get("category2"), "test2");
			Assert.assertEquals(((Map<String, Object>) ((Map<String, Object>) result.get("data")).get("set1")).get("category3"), "test3");
			Assert.assertEquals(((Map<String, Object>) result.get("data")).get("set2"), "test5");
		}

		@Test
		public void testWrongSeperator() throws Exception {
			this.separator = "--";
			this.normalize = false;

			final Map<String, Object> result = new HashMap<String, Object>();
			final HBaseParser parser = new HBaseParser(new HBaseRiver(null, null, null));
			parser.readQualifierStructure(result, "data::set1::category1", "test1");
			parser.readQualifierStructure(result, "data::set1::category2", "test2");
			parser.readQualifierStructure(result, "data::set1::category3", "test3");
			this.normalize = true;
			parser.readQualifierStructure(result, "data::set2::category1", "test4");
			parser.readQualifierStructure(result, "data::set2::category2", "test5");

			Assert.assertEquals(result.get("data::set1::category1"), "test1");
			Assert.assertEquals(result.get("data::set1::category2"), "test2");
			Assert.assertEquals(result.get("data::set1::category3"), "test3");
			Assert.assertEquals(result.get("dataset2category1"), "test4");
			Assert.assertEquals(result.get("dataset2category2"), "test5");
		}
	}

	public class ReadDataTreeTest {
		private final Charset	charset		= Charset.forName("UTF-8");
		private int				rowCounter	= 0;

		@BeforeClass
		public void setUp() {
			new MockUp<AbstractRiverComponent>() {
				@Mock
				void $init(final RiverName riverName, final RiverSettings settings) {}
			};

			new MockUp<HBaseRiver>() {

				@Mock
				void $init(final RiverName riverName, final RiverSettings settings, final Client esClient) {}

				@Mock
				Charset getCharset() {
					return ReadDataTreeTest.this.charset;
				}

				@Mock
				boolean isNormalizeFields() {
					return true;
				}
			};
		}

		@AfterClass
		public void tearDown() {
			Mockit.tearDownMocks();
		}

		@Test
		@SuppressWarnings("unchecked")
		public void testBase() {
			final HBaseParser parser = new HBaseParser(new HBaseRiver(null, null, null));

			final ArrayList<KeyValue> input = new ArrayList<KeyValue>();

			input.add(getKeyValue("family1", "category1", "value1"));
			input.add(getKeyValue("family1", "category2", "value2"));
			input.add(getKeyValue("family1", "category3", "value3"));
			input.add(getKeyValue("family2", "category1", "value4"));
			input.add(getKeyValue("family2", "category4", "value5"));
			input.add(getKeyValue("family3", "category5", "value6"));
			input.add(getKeyValue("family2", "category6", "value7"));

			final Map<String, Object> output = parser.readDataTree(input);

			Assert.assertNotNull(output.get("family1"));
			final Map<String, Object> family1 = (Map<String, Object>) output.get("family1");
			Assert.assertEquals(family1.get("category1"), "value1");
			Assert.assertEquals(family1.get("category2"), "value2");
			Assert.assertEquals(family1.get("category3"), "value3");
			Assert.assertNotNull(output.get("family2"));
			final Map<String, Object> family2 = (Map<String, Object>) output.get("family2");
			Assert.assertEquals(family2.get("category1"), "value4");
			Assert.assertEquals(family2.get("category4"), "value5");
			Assert.assertEquals(family2.get("category6"), "value7");
			Assert.assertNotNull(output.get("family3"));
			final Map<String, Object> family3 = (Map<String, Object>) output.get("family3");
			Assert.assertEquals(family3.get("category5"), "value6");
		}

		private KeyValue getKeyValue(final String family, final String qualifier, final String value) {
			return new KeyValue(String.valueOf(this.rowCounter++).getBytes(this.charset),
				family.getBytes(this.charset),
				qualifier.getBytes(this.charset),
				value.getBytes(this.charset));
		}
	}
}
