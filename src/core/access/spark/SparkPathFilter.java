package core.access.spark;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import core.access.AccessMethod;
import core.access.Partition;
import core.adapt.Predicate;

public class SparkPathFilter implements PathFilter, Configurable  {

	protected Configuration conf;
	protected AccessMethod am;

	protected Predicate[] predicates;
	protected Partition partition;

	public void setConf(Configuration conf) {
		this.conf = conf;
		SparkQueryConf queryConf = new SparkQueryConf(conf);
		am = new AccessMethod();
		am.init(queryConf.getDataset());
		predicates = queryConf.getPredicates();
		partition = new Partition("");
	}

	public boolean accept(Path arg0) {
		partition.setPath(FilenameUtils.getName(arg0.toString()));
		for(Predicate predicate: predicates)
			if(!am.isRelevant(partition, predicate))
				return false;
		return true;
	}

	public Configuration getConf() {
		return conf;
	}
}
