package p.minn.lucene.service;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import p.minn.common.utils.MyGsonMap;
import p.minn.hadoop.hdfs.HDFSFileUtils;

/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
@Service
public class LuceneHadoopService {

	@Autowired
	private HDFSFileUtils hdfsFileUtils;

	public Object add(String messageBody, String lang) throws Exception {
		MyGsonMap map = MyGsonMap.getInstance(messageBody, Map.class, Map.class);
		Map param = (Map) map.gson2Map();
		Map<String, Object> rs = new HashMap<String, Object>();
		hdfsFileUtils.setInput("lucene");
		HdfsDirectory dhfs = hdfsFileUtils.getHdfsDirectory("");
		File disk = null;
		Analyzer analyzer = new SimpleAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, analyzer);
		IndexWriter writer = new IndexWriter(dhfs, config);
		List data = new ArrayList();
		Document document = new Document();
		document.add(new StringField("name", param.get("name").toString(), Store.YES));
		document.add(new StringField("age", param.get("age").toString(), Store.YES));
		writer.addDocument(document);
		Map<String, String> d = new HashMap<String, String>();
		d.put("id", "1");
		d.put("name", param.get("name").toString());
		d.put("age", param.get("age").toString());
		data.add(d);
		writer.forceMerge(1);
		writer.commit();
		writer.close();

		rs.put("data", data);
		rs.put("info", "ok");
		return rs;
	}

	public Object query(String messageBody, String lang) throws Exception {
		MyGsonMap map = MyGsonMap.getInstance(messageBody, Map.class, Map.class);
		Map param = (Map) map.gson2Map();
		Analyzer analyzer = new StandardAnalyzer();
		hdfsFileUtils.setInput("lucene");
		HdfsDirectory hdfs = hdfsFileUtils.getHdfsDirectory("");
		Map result = new HashMap();
		IndexSearcher indexSearcher = null;
		IndexReader reader = DirectoryReader.open(hdfs);
		indexSearcher = new IndexSearcher(reader);
		String name = param.get("name").toString();
		TermQuery query = new TermQuery(new Term("name", name));
		BooleanQuery bq = new BooleanQuery();
		bq.add(query, BooleanClause.Occur.MUST);
		result.put("age", "");
		TopDocs ds = indexSearcher.search(bq, 50);
		result.put("hits", ds.totalHits);
		for (int i = 0; i < ds.totalHits; i++) {
			if (result.get("age").toString().length() > 0) {
				result.put("age", result.get("age").toString() + "," + indexSearcher.doc(i).get("age"));
			} else {
				result.put("age", indexSearcher.doc(i).get("age"));
			}
		}

		reader.close();
		hdfs.close();
		result.put("info", "ok");
		return result;
	}

}
