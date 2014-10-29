package com.ninja.hadoop.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

public final class VertexWritable implements Writable, Cloneable {

    private Text vertexId;
    private TreeSet<Text> edges;
    private boolean activated;
    private Text sideData;

    public VertexWritable() {
        super();
    }

    public VertexWritable(Text minimalVertexId) {
        super();
        this.vertexId = minimalVertexId;
    }

    // true if updated
    public boolean checkAndSetMinimalVertex(Text id) {
        if (vertexId == null) {
            vertexId = id;
            return true;
        } else {
            if (id.toString().compareTo(vertexId.toString()) < 0) {
                vertexId = id;
                return true;
            }
        }
        return false;
    }

    public boolean isMessage() {
        if (edges == null)
            return true;
        else
            return false;
    }

    public VertexWritable makeMessage() {
        return new VertexWritable(vertexId);
    }

    public void addVertex(Text id) {
        if (edges == null)
            edges = new TreeSet<Text>();
        edges.add(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vertexId.write(out);
        if (edges == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(edges.size());
            for (Text t : edges) {
                t.write(out);
            }
        }
        out.writeBoolean(activated);
        if (sideData == null) {
            out.write(-1);
        } else {
            sideData.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexId = new Text();
        vertexId.readFields(in);
        int length = in.readInt();
        if (length > -1) {
            edges = new TreeSet<Text>();
            for (int i = 0; i < length; i++) {
                Text temp = new Text();
                temp.readFields(in);
                edges.add(temp);
            }
        } else {
            edges = null;
        }
        activated = in.readBoolean();
        sideData = new Text();
        if (length > -1) {
            sideData.readFields(in);
        }
    }

    @Override
    public String toString() {
        return "VertexWritable [minimalVertexId=" + vertexId + ", pointsTo="
                + edges + "]" + " [side data : "+ sideData +"]";
    }

    @Override
    protected VertexWritable clone() {
        VertexWritable toReturn = new VertexWritable(new Text(
                vertexId.toString()));
        if (edges != null) {
            toReturn.edges = new TreeSet<Text>();
            for (Text t : edges) {
                toReturn.edges.add(new Text(t.toString()));
            }
        }
        if (sideData != null){
            toReturn.setSideData(sideData);
        }
        return toReturn;
    }

    public Text getSideData() {
        return sideData;
    }

    public void setSideData(Text sideData) {
        this.sideData = sideData;
    }

    public boolean isActivated() {
        return activated;
    }

    public void setVertexId(Text vertexId) {
        this.vertexId = vertexId;
    }

    public void setEdges(TreeSet<Text> edges) {
        this.edges = edges;
    }

    public void setActivated(boolean activated) {
        this.activated = activated;
    }

    public Text getVertexId() {
        return vertexId;
    }

    public TreeSet<Text> getEdges() {
        return edges;
    }

}
