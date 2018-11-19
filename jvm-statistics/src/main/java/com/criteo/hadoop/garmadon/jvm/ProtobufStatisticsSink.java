package com.criteo.hadoop.garmadon.jvm;

import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;

public class ProtobufStatisticsSink implements StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> {

    private JVMStatisticsEventsProtos.JVMStatisticsData.Builder dataBuilder = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder();
    private JVMStatisticsEventsProtos.JVMStatisticsData.Section.Builder sectionBuilder;

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> beginSection(String name) {
        sectionBuilder = dataBuilder.addSectionBuilder();
        sectionBuilder.setName(name);
        return this;
    }

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> endSection() {
        sectionBuilder.build();
        return this;
    }

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> addDuration(String property, long timeInMilli) {
        return add(property, String.valueOf(timeInMilli));
    }

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> addSize(String property, long sizeInBytes) {
        return add(property, String.valueOf(sizeInBytes / 1024));
    }

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> addPercentage(String property, int percent) {
        return add("%" + property, String.valueOf(percent));
    }

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> add(String property, String value) {
        if (sectionBuilder == null) sectionBuilder = dataBuilder.addSectionBuilder();
        JVMStatisticsEventsProtos.JVMStatisticsData.Property.Builder propertyBuilder = sectionBuilder.addPropertyBuilder();
        propertyBuilder.setName(property);
        if (value != null) propertyBuilder.setValue(value);
        propertyBuilder.build();
        return this;
    }

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> add(String property, int value) {
        return add(property, String.valueOf(value));
    }

    @Override
    public StatisticsSink<JVMStatisticsEventsProtos.JVMStatisticsData> add(String property, long value) {
        return add(property, String.valueOf(value));
    }

    @Override
    public JVMStatisticsEventsProtos.JVMStatisticsData flush() {
        JVMStatisticsEventsProtos.JVMStatisticsData data = dataBuilder.build();
        dataBuilder = JVMStatisticsEventsProtos.JVMStatisticsData.newBuilder();
        return data;
    }
}
