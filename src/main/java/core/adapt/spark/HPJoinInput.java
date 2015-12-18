package core.adapt.spark;

/**
 * Created by ylu on 12/3/15.
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import core.adapt.AccessMethod;
import core.adapt.Predicate;
import core.adapt.AccessMethod.PartitionSplit;
import core.adapt.Query;
import core.adapt.iterator.PostFilterIterator;
import core.adapt.iterator.RepartitionIterator;

public class HPJoinInput {

    protected AccessMethod am;
    protected Map<Integer, FileStatus> partitionIdFileMap;
    protected boolean MDIndexInput;


    public HPJoinInput(boolean MDIndexInput){
        this.MDIndexInput = MDIndexInput;
    }

    public void initialize(List<FileStatus> files, AccessMethod am) {
        this.am = am;
        initialize(files);
    }

    public void initialize(List<FileStatus> files) {
        partitionIdFileMap = new HashMap<Integer, FileStatus>();
        for (FileStatus file : files) {
            System.out.println("FILE: " + file.getPath());
            try {
                String fileName = FilenameUtils.getName(file.getPath().toString());
                int id = 0;
                if(MDIndexInput) {
                    id = Integer.parseInt(fileName);
                } else {
                    id = Integer.parseInt(fileName.substring(fileName.indexOf('-') + 1));
                }
                partitionIdFileMap.put(id, file);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
    }

    public PartitionSplit[] getFullScan(Query q) {
        return new PartitionSplit[]{new PartitionSplit(
                Ints.toArray(partitionIdFileMap.keySet()),
                new PostFilterIterator(q))};
    }

    public PartitionSplit[] getRepartitionScan(Query q) {
        return new PartitionSplit[]{new PartitionSplit(
                Ints.toArray(partitionIdFileMap.keySet()),
                new RepartitionIterator(q))};
    }

    public PartitionSplit[] getIndexScan(boolean justAccess,
                                         Query q) {
        return am.getPartitionSplits(q, justAccess);
    }

    // utility methods

    public Path[] getPaths(ArrayList<Integer> partitionIds) {
        Path[] splitFilesArr = new Path[partitionIds.size()];
        for (int i = 0; i < splitFilesArr.length; i++)
            splitFilesArr[i] = partitionIdFileMap.get(partitionIds.get(i)).getPath();
        return splitFilesArr;
    }



    public long[] getLengths(ArrayList<Integer>  partitionIds) {
        long[] lengthsArr = new long[partitionIds.size()];
        for (int i = 0; i < lengthsArr.length; i++)
            lengthsArr[i] = partitionIdFileMap.get(partitionIds.get(i)).getLen();
        return lengthsArr;
    }

}
