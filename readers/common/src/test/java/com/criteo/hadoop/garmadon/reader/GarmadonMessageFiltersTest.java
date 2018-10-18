package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;
import org.junit.Test;

import java.util.Random;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class GarmadonMessageFiltersTest {

    private final Random random = new Random();

    @Test
    public void Disjunction_should_accept_either_left_true_or_right_true(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.or(right);

        when(left.accepts(anyInt(), any())).thenReturn(true);
        when(right.accepts(anyInt(), any())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt(), EventHeaderProtos.Header.getDefaultInstance()), is(true));

        when(left.accepts(anyInt(), any())).thenReturn(true);
        when(right.accepts(anyInt(), any())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt(), EventHeaderProtos.Header.getDefaultInstance()), is(true));

        when(left.accepts(anyInt(), any())).thenReturn(false);
        when(right.accepts(anyInt(), any())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt(), EventHeaderProtos.Header.getDefaultInstance()), is(true));

        when(left.accepts(anyInt(), any())).thenReturn(false);
        when(right.accepts(anyInt(), any())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt(), EventHeaderProtos.Header.getDefaultInstance()), is(false));
    }

    @Test
    public void Disjunction_should_accept_either_left_true_or_right_true_or_both_for_types(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.or(right);

        when(left.accepts(anyInt())).thenReturn(true);
        when(right.accepts(anyInt())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt()), is(true));

        when(left.accepts(anyInt())).thenReturn(true);
        when(right.accepts(anyInt())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt()), is(true));

        when(left.accepts(anyInt())).thenReturn(false);
        when(right.accepts(anyInt())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt()), is(true));

        when(left.accepts(anyInt())).thenReturn(false);
        when(right.accepts(anyInt())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt()), is(false));
    }

    @Test
    public void Disjunction_should_not_evaluate_right_unless_left_is_false(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.or(right);

        when(left.accepts(anyInt(), any())).thenReturn(true);
        filter.accepts(random.nextInt(), EventHeaderProtos.Header.getDefaultInstance());
        verify(right, never()).accepts(anyInt(), any());

        when(left.accepts(anyInt(), any())).thenReturn(false);
        filter.accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance());
        verify(right).accepts(anyInt(), any());
    }

    @Test
    public void Disjunction_should_not_evaluate_right_unless_left_is_false_for_types(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.or(right);

        when(left.accepts(anyInt())).thenReturn(true);
        filter.accepts(random.nextInt());
        verify(right, never()).accepts(anyInt());

        when(left.accepts(anyInt())).thenReturn(false);
        filter.accepts(random.nextInt());
        verify(right).accepts(anyInt());
    }

    @Test
    public void Conjunction_should_accept_left_true_and_right_true_only(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.and(right);

        when(left.accepts(anyInt(), any())).thenReturn(true);
        when(right.accepts(anyInt(), any())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance()), is(true));

        when(left.accepts(anyInt(), any())).thenReturn(true);
        when(right.accepts(anyInt(), any())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance()), is(false));

        when(left.accepts(anyInt(), any())).thenReturn(false);
        when(right.accepts(anyInt(), any())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance()), is(false));

        when(left.accepts(anyInt(), any())).thenReturn(false);
        when(right.accepts(anyInt(), any())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance()), is(false));
    }

    @Test
    public void Conjunction_should_accept_left_true_and_right_true_only_for_types(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.and(right);

        when(left.accepts(anyInt())).thenReturn(true);
        when(right.accepts(anyInt())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt()), is(true));

        when(left.accepts(anyInt())).thenReturn(true);
        when(right.accepts(anyInt())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt()), is(false));

        when(left.accepts(anyInt())).thenReturn(false);
        when(right.accepts(anyInt())).thenReturn(true);
        assertThat(filter.accepts(random.nextInt()), is(false));

        when(left.accepts(anyInt())).thenReturn(false);
        when(right.accepts(anyInt())).thenReturn(false);
        assertThat(filter.accepts(random.nextInt()), is(false));
    }

    @Test
    public void Conjunction_should_evaluate_all_operands(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.and(right);

        when(left.accepts(anyInt(), any())).thenReturn(true);
        filter.accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance());
        verify(right).accepts(anyInt(), any());

        when(left.accepts(anyInt(), any())).thenReturn(false);
        filter.accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance());
        verify(right).accepts(anyInt(), any());
    }

    @Test
    public void Conjunction_should_evaluate_all_operands_for_types(){
        GarmadonMessageFilter.Junction left = newMockableJunction();
        GarmadonMessageFilter.Junction right = newMockableJunction();
        GarmadonMessageFilter.Junction filter = left.and(right);

        when(left.accepts(anyInt())).thenReturn(true);
        filter.accepts(random.nextInt());
        verify(right).accepts(anyInt());

        when(left.accepts(anyInt())).thenReturn(false);
        filter.accepts(random.nextInt());
        verify(right).accepts(anyInt());
    }

    @Test
    public void TagFilter_should_accept_only_header_with_correct_tag(){
        GarmadonMessageFilter.HeaderFilter nodemanagerFilter = GarmadonMessageFilters.hasTag(Header.Tag.NODEMANAGER);
        GarmadonMessageFilter.HeaderFilter forwarderFilter = GarmadonMessageFilters.hasTag(Header.Tag.FORWARDER);

        EventHeaderProtos.Header noTag = EventHeaderProtos.Header.newBuilder().build();
        EventHeaderProtos.Header tagForwarder = EventHeaderProtos.Header.newBuilder().addTags(Header.Tag.FORWARDER.toString()).build();
        EventHeaderProtos.Header tagNodemanager = EventHeaderProtos.Header.newBuilder().addTags(Header.Tag.NODEMANAGER.toString()).build();

        assertThat(nodemanagerFilter.accepts(random.nextInt(),noTag), is(false));
        assertThat(nodemanagerFilter.accepts(random.nextInt(),tagForwarder), is(false));
        assertThat(nodemanagerFilter.accepts(random.nextInt(),tagNodemanager), is(true));

        assertThat(forwarderFilter.accepts(random.nextInt(),noTag), is(false));
        assertThat(forwarderFilter.accepts(random.nextInt(),tagForwarder), is(true));
        assertThat(forwarderFilter.accepts(random.nextInt(),tagNodemanager), is(false));
    }

    @Test
    public void TagFilter_should_accept_all_types(){
        GarmadonMessageFilter.HeaderFilter filter = GarmadonMessageFilters.hasTag(Header.Tag.NODEMANAGER);
        IntStream.of(20).forEach(i -> assertThat(filter.accepts(random.nextInt()), is(true)));
    }

    @Test
    public void TypeFilter_should_accept_only_designed_type(){
        int type = GarmadonSerialization.TypeMarker.STATE_EVENT;
        GarmadonMessageFilter.TypeFilter filter = GarmadonMessageFilters.hasType(type);

        IntStream.of(20).forEach(i -> {
            int n = random.nextInt();
            if(n != type) {
                assertThat(filter.accepts(n), is(false));
            }
        });

        assertThat(filter.accepts(type), is(true));
    }

    @Test
    public void NotFilter_should_negate_wrapped_filter(){
        GarmadonMessageFilter filter = mock(GarmadonMessageFilter.class);

        when(filter.accepts(anyInt())).thenReturn(true);
        when(filter.accepts(anyInt(), any())).thenReturn(true);
        assertThat(GarmadonMessageFilters.not(filter).accepts(random.nextInt(), EventHeaderProtos.Header.getDefaultInstance()), is(false));
        assertThat(GarmadonMessageFilters.not(filter).accepts(random.nextInt()), is(false));

        when(filter.accepts(anyInt())).thenReturn(false);
        when(filter.accepts(anyInt(), any())).thenReturn(false);
        assertThat(GarmadonMessageFilters.not(filter).accepts(random.nextInt(),EventHeaderProtos.Header.getDefaultInstance()), is(true));
        assertThat(GarmadonMessageFilters.not(filter).accepts(random.nextInt()), is(true));
    }

    @Test
    public void Disjunction_with_TypeFilter_and_HeaderFilter_should_work(){
        GarmadonMessageFilter.HeaderFilter hFilter = newMockableHeaderFilter();
        int type = GarmadonSerialization.TypeMarker.STATE_EVENT;
        GarmadonMessageFilter.TypeFilter tFilter = GarmadonMessageFilters.hasType(type);

        GarmadonMessageFilter dis = hFilter.or(tFilter);
        /* accept with type only should always pass */
        /* This si due to the fact that to test header, we must pass the first test on type */
        assertThat(dis.accepts(random.nextInt()), is(true));

        /* If the header is not accepted, maybe we must still accept because the type was correct */
        when(hFilter.accepts(any())).thenReturn(false);
        assertThat(dis.accepts(type - 1, EventHeaderProtos.Header.getDefaultInstance()), is(false));
        assertThat(dis.accepts(type, EventHeaderProtos.Header.getDefaultInstance()), is(true));
    }

    private GarmadonMessageFilter.Junction.AbstractBase newMockableJunction() {
        return mock(GarmadonMessageFilter.Junction.AbstractBase.class, withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
    }

    private GarmadonMessageFilter.HeaderFilter newMockableHeaderFilter() {
        return mock(GarmadonMessageFilter.HeaderFilter.class, withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
    }

}
