package core.adapt.iterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.io.ByteStreams;

import core.adapt.Query;

public class PostFilterIterator extends PartitionIterator implements
		Serializable {
	private static final long serialVersionUID = 1L;

	protected Query query;

	public PostFilterIterator() {
	}

	public PostFilterIterator(String iteratorString) {
		try {
			readFields(ByteStreams.newDataInput(iteratorString.getBytes()));
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to read the fields");
		}
	}

	public PostFilterIterator(Query query) {
		this.query = query;
	}

	@Override
	protected boolean isRelevant(IteratorRecord record) {
		return query.qualifies(record);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		query.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		query = new Query(); // hard coded
		query.readFields(in);
	}

	public static PostFilterIterator read(DataInput in) throws IOException {
		PostFilterIterator it = new PostFilterIterator();
		it.readFields(in);
		return it;
	}
}
