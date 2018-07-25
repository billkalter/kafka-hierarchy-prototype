package com.bazaarvoice.legion.hierarchy.model;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

public class HierarchySerdes {

    abstract private static class AbstractJavaSerializer<C extends Serializable> implements Serializer<C> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // no-op
        }

        @Override
        public byte[] serialize(String topic, C data) {
            if (data == null) {
                return null;
            } else {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                try (ObjectOutputStream out = new ObjectOutputStream(bout)) {
                    out.writeObject(data);
                } catch (IOException e) {
                    // Shouldn't happen
                    throw new RuntimeException(e);
                }
                return bout.toByteArray();
            }
        }

        @Override
        public void close() {
            // no-op
        }
    }

    abstract private static class AbstractJavaDeserializer<C extends Serializable> implements Deserializer<C> {
        private final Class<C> _deserClass;

        protected AbstractJavaDeserializer(Class<C> deserClass) {
            _deserClass = deserClass;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // no-op
        }

        @Override
        public C deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            } else {
                try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
                    return _deserClass.cast(in.readObject());
                } catch (IOException | ClassNotFoundException e) {
                    // Shouldn't happen
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void close() {
            // no-op
        }
    }

    public static final class ParentTransitionSerializer extends AbstractJavaSerializer<ParentTransition> {};
    public static final class ParentTransitionDeserializer extends AbstractJavaDeserializer<ParentTransition> {
        public ParentTransitionDeserializer() {
            super(ParentTransition.class);
        }
    }

    public static Serde<ParentTransition> ParentTransition() {
        return Serdes.serdeFrom(new ParentTransitionSerializer(), new ParentTransitionDeserializer());
    }

    public static final class ChildTransitionSerializer extends AbstractJavaSerializer<ChildTransition> {};
    public static final class ChildTransitionDeserializer extends AbstractJavaDeserializer<ChildTransition> {
        public ChildTransitionDeserializer() {
            super(ChildTransition.class);
        }
    };

    public static Serde<ChildTransition> ChildTransition() {
        return Serdes.serdeFrom(new ChildTransitionSerializer(), new ChildTransitionDeserializer());
    }

    public static final class ChildIdSetSerializer extends AbstractJavaSerializer<ChildIdSet> {};
    public static final class ChildIdSetDeserializer extends AbstractJavaDeserializer<ChildIdSet> {
        public ChildIdSetDeserializer() {
            super(ChildIdSet.class);
        }
    }

    public static Serde<ChildIdSet> ChildIdSet() {
        return Serdes.serdeFrom(new ChildIdSetSerializer(), new ChildIdSetDeserializer());
    }

    public static final class LineageSerializer extends AbstractJavaSerializer<Lineage> {};
    public static final class LineageDeserializer extends AbstractJavaDeserializer<Lineage> {
        public LineageDeserializer() {
            super(Lineage.class);
        }
    }

    public static Serde<Lineage> Lineage() {
        return Serdes.serdeFrom(new LineageSerializer(), new LineageDeserializer());
    }

    public static final class LineageTransitionSerializer extends AbstractJavaSerializer<LineageTransition> {};
    public static final class LineageTransitionDeserializer extends AbstractJavaDeserializer<LineageTransition> {
        public LineageTransitionDeserializer() {
            super(LineageTransition.class);
        }
    }

    public static Serde<LineageTransition> LineageTransition() {
        return Serdes.serdeFrom(new LineageTransitionSerializer(), new LineageTransitionDeserializer());
    }

}
