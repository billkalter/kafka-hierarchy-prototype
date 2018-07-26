package com.bazaarvoice.legion.hierarchy;

import com.bazaarvoice.legion.hierarchy.model.ChildIdSet;
import com.bazaarvoice.legion.hierarchy.model.ChildTransition;
import com.bazaarvoice.legion.hierarchy.model.HierarchySerdes;
import com.bazaarvoice.legion.hierarchy.model.Lineage;
import com.bazaarvoice.legion.hierarchy.model.LineageTransition;
import com.bazaarvoice.legion.hierarchy.model.ParentTransition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TopologyGenerator {

    private final static Logger _log = LoggerFactory.getLogger(TopologyGenerator.class);

    public Topology createTopology(HierarchyStreamConfig config) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> sourceTable = builder.table(
                config.getSourceTopic(), Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, ParentTransition> parentTransitionTable = builder.table(
                config.getParentTransitionTopic(), Consumed.with(Serdes.String(), HierarchySerdes.ParentTransition()));

        KStream<String, ChildTransition> childTransitionStream = builder.stream(
                config.getChildTransitionTopic(), Consumed.with(Serdes.String(), HierarchySerdes.ChildTransition()));

        KTable<String, ChildIdSet> parentChildrenTable = builder.table(
                config.getParentChildrenTopic(), Consumed.with(Serdes.String(), HierarchySerdes.ChildIdSet()));

        KTable<String, Lineage> destTable = builder.table(
                config.getDestTopic(), Consumed.with(Serdes.String(), HierarchySerdes.Lineage()));

        // Index 0 will contain all children which set themselves as their own parent
        // Index 1 will contain all otherwise valid child/parent relationships
        @SuppressWarnings("unchecked")
        KStream<String, String>[] splitSources = sourceTable
                .toStream()
                .branch(Objects::equals,
                        (childId, parentId) -> true);

        // Child who are their own parents are immediately undefined
        splitSources[0]
                .map((childId, ignore) -> new KeyValue<>(childId, new Lineage(Reserved.UNDEFINED)))
                .to(config.getDestTopic(), Produced.with(Serdes.String(), HierarchySerdes.Lineage()));

        splitSources[1]
                .leftJoin(
                        parentTransitionTable,
                        (newParentId, priorTransition) -> new ParentTransition(
                                Optional.ofNullable(priorTransition).map(ParentTransition::getNewParentId).orElse(null),
                                parentOrRoot(newParentId)))
                .filter((child, parentTransition) -> !Objects.equals(parentTransition.getOldParentId(), parentTransition.getNewParentId()))
                .to(config.getParentTransitionTopic(), Produced.with(Serdes.String(), HierarchySerdes.ParentTransition()));

        parentTransitionTable
                .toStream()
                .flatMap((KeyValueMapper<String, ParentTransition, Iterable<? extends KeyValue<String, ChildTransition>>>) (childId, parentTransition) -> {
                    List<KeyValue<String, ChildTransition>> transitions = new ArrayList<>(2);
                    if (parentTransition.getOldParentId() != null) {
                        transitions.add(new KeyValue<>(parentTransition.getOldParentId(), new ChildTransition(childId, false)));
                    }
                    transitions.add(new KeyValue<>(parentTransition.getNewParentId(), new ChildTransition(childId, true)));
                    return transitions;
                })
                .to(config.getChildTransitionTopic(), Produced.with(Serdes.String(), HierarchySerdes.ChildTransition()));

        // Root parents
        childTransitionStream
                .filter((id, transition) -> isRoot(id) && transition.isAdd())
                .map((id, transition) -> new KeyValue<>(transition.getChildId(), Lineage.EMPTY))
                .to(config.getDestTopic(), Produced.with(Serdes.String(), HierarchySerdes.Lineage()));

        // Parents which don't yet exist
        childTransitionStream
                .leftJoin(sourceTable, (childTransition, maybeParent) -> maybeParent != null)
                .filter((id, exists) -> !(exists || isRoot(id)))
                .map((id, ignore) -> new KeyValue<>(id, new Lineage(Reserved.UNDEFINED)))
                .to(config.getDestTopic(), Produced.with(Serdes.String(), HierarchySerdes.Lineage()));

        childTransitionStream
                .groupByKey(Serialized.with(Serdes.String(), HierarchySerdes.ChildTransition()))
                .aggregate(
                        () -> ChildIdSet.EMPTY,
                        (key, t, idSet) -> idSet.withUpdate(t.getChildId(), t.isAdd()),
                        Materialized.with(Serdes.String(), HierarchySerdes.ChildIdSet()))
                .toStream()
                .to(config.getParentChildrenTopic(), Produced.with(Serdes.String(), HierarchySerdes.ChildIdSet()));

        parentChildrenTable
                .join(destTable, (children, lineage) -> new LineageTransition(lineage.getLinage(), children.children()))
                .toStream()
                .flatMap((KeyValueMapper<String, LineageTransition, Iterable<? extends KeyValue<String, Lineage>>>) (id, lineageTransition) -> {
                    if (lineageTransition.getParentLineage().contains(id)) {
                        _log.warn("Rejected update which creates a lineage cycle for {}", id);
                        return Collections.emptyList();
                    }
                    List<String> updatedLinage = new ArrayList<>(lineageTransition.getParentLineage());
                    updatedLinage.add(id);
                    return lineageTransition.getChildren().stream()
                            .map(childId -> new KeyValue<>(childId, new Lineage(updatedLinage)))
                            .collect(Collectors.toList());
                })
                .to(config.getDestTopic(), Produced.with(Serdes.String(), HierarchySerdes.Lineage()));

        return builder.build();
    }

    private static String parentOrRoot(String parentId) {
        return Optional.ofNullable(parentId).orElse(Reserved.ROOT);
    }

    private static boolean isRoot(String id) {
        return Reserved.ROOT.equals(id);
    }
}
