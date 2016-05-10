package core.adapt;


import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import core.adapt.Predicate;
import core.adapt.Query;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

/**
 * Created by ylu on 1/25/16.
 */

public class JoinQuery implements Serializable {
    private static final long serialVersionUID = 1L;

    private Predicate[] predicates;
    private String table;
    private int joinAttribute;

    public JoinQuery(String queryString) {
        String[] parts = queryString.split("\\|");
        this.table = parts[0];
        this.joinAttribute = Integer.parseInt(parts[1]);
        if (parts.length > 2) {
            String predString = parts[2].trim();
            String[] predParts = predString.split(";");
            this.predicates = new Predicate[predParts.length];
            for (int i = 0; i < predParts.length; i++) {
                this.predicates[i] = new Predicate(predParts[i]);
            }
        } else {
            this.predicates = new Predicate[0];
        }
    }

    public JoinQuery(String table, int joinAttribute, Predicate[] predicates) {
        this.table = table;
        this.joinAttribute = joinAttribute;
        this.predicates = predicates;
    }

    public Predicate[] getPredicates() {
        return this.predicates;
    }

    public String getTable() {
        return this.table;
    }

    public int getJoinAttribute(){
        return this.joinAttribute;
    }

    public Query castToQuery(){
        return new Query(table, predicates);
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toString());
    }



    @Override
    public String toString() {
        String stringPredicates = "";
        if (predicates.length != 0)
            stringPredicates = Joiner.on(";").join(predicates);
        return table + "|" + joinAttribute  + "|" +  stringPredicates;
    }
}
