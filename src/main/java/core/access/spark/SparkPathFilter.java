package core.access.spark;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import core.access.AccessMethod;
import core.access.Predicate;

public class SparkPathFilter implements PathFilter, Configurable {

	protected Configuration conf;
	protected AccessMethod am;
	protected Predicate[] predicates;

	public void setConf(Configuration conf) {
		this.conf = conf;
		SparkQueryConf queryConf = new SparkQueryConf(conf);
		am = new AccessMethod();
		am.init(queryConf);
		predicates = queryConf.getQuery();
	}

	public boolean accept(Path arg0) {
		for (Predicate predicate : predicates)
			if (!am.isRelevant(FilenameUtils.getName(arg0.toString()),
					predicate))
				return false;
		return true;
	}

	public Configuration getConf() {
		return conf;
	}
}
