package com.criteo.jvm;

public class ProtobufStatisticsSink implements StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> {

    private JVMStatisticsProtos.JVMStatisticsData.Builder dataBuilder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
    private JVMStatisticsProtos.JVMStatisticsData.Section.Builder sectionBuilder;

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> beginSection(String name) {
        sectionBuilder = dataBuilder.addSectionBuilder();
        sectionBuilder.setName(name);
        return this;
    }

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> endSection() {
        sectionBuilder.build();
        return this;
    }

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> addDuration(String property, long timeInMilli) {
        return add(property, String.valueOf(timeInMilli));
    }

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> addSize(String property, long sizeInBytes) {
        return add(property, String.valueOf(sizeInBytes/1024));
    }

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> addPercentage(String property, int percent) {
        return add( "%" + property, String.valueOf(percent));
    }

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> add(String property, String value) {
        if (sectionBuilder == null)
            sectionBuilder = dataBuilder.addSectionBuilder();
        JVMStatisticsProtos.JVMStatisticsData.Property.Builder propertyBuilder = sectionBuilder.addPropertyBuilder();
        propertyBuilder.setName(property);
        if (value != null)
            propertyBuilder.setValue(value);
        propertyBuilder.build();
        return this;
    }

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> add(String property, int value) {
        return add(property, String.valueOf(value));
    }

    @Override
    public StatisticsSink<JVMStatisticsProtos.JVMStatisticsData> add(String property, long value) {
        return add(property, String.valueOf(value));
    }

    @Override
    public JVMStatisticsProtos.JVMStatisticsData flush() {
        dataBuilder.setTimestamp(System.currentTimeMillis());
        JVMStatisticsProtos.JVMStatisticsData data = dataBuilder.build();
        dataBuilder = JVMStatisticsProtos.JVMStatisticsData.newBuilder();
        return data;
    }
}
