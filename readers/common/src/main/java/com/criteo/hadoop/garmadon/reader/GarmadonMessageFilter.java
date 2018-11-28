package com.criteo.hadoop.garmadon.reader;

import com.criteo.hadoop.garmadon.event.proto.EventHeaderProtos;
import com.criteo.hadoop.garmadon.schema.events.Header;

public interface GarmadonMessageFilter {

    boolean accepts(int type);

    boolean accepts(int type, EventHeaderProtos.Header header);

    interface Junction extends GarmadonMessageFilter {

        Junction and(GarmadonMessageFilter other);

        Junction or(GarmadonMessageFilter other);

        abstract class AbstractBase implements Junction {

            @Override
            public Junction and(GarmadonMessageFilter other) {
                if (other == null) throw new NullPointerException("Null filter is forbidden");
                return new Conjunction(this, other);
            }

            @Override
            public Junction or(GarmadonMessageFilter other) {
                if (other == null) throw new NullPointerException("Null filter is forbidden");
                return new Disjunction(this, other);
            }

        }

        class Conjunction extends AbstractBase {

            private final GarmadonMessageFilter left;
            private final GarmadonMessageFilter right;

            public Conjunction(GarmadonMessageFilter left, GarmadonMessageFilter right) {
                this.left = left;
                this.right = right;
            }

            @Override
            public boolean accepts(int type) {
                return left.accepts(type) && right.accepts(type);
            }

            @Override
            public boolean accepts(int type, EventHeaderProtos.Header header) {
                return left.accepts(type, header) && right.accepts(type, header);
            }

        }

        class Disjunction extends AbstractBase {

            private final GarmadonMessageFilter left;
            private final GarmadonMessageFilter right;

            public Disjunction(GarmadonMessageFilter left, GarmadonMessageFilter right) {
                this.left = left;
                this.right = right;
            }

            @Override
            public boolean accepts(int type) {
                return left.accepts(type) || right.accepts(type);
            }

            @Override
            public boolean accepts(int type, EventHeaderProtos.Header header) {
                return left.accepts(type, header) || right.accepts(type, header);
            }

        }

    }

    enum ANY implements GarmadonMessageFilter {

        INSTANCE;

        @Override
        public boolean accepts(int type) {
            return true;
        }

        @Override
        public boolean accepts(int type, EventHeaderProtos.Header header) {
            return true;
        }
    }

    enum NONE implements GarmadonMessageFilter {

        INSTANCE;

        @Override
        public boolean accepts(int type) {
            return false;
        }

        @Override
        public boolean accepts(int type, EventHeaderProtos.Header header) {
            return false;
        }
    }

    class NotFilter extends Junction.AbstractBase implements GarmadonMessageFilter {

        private final GarmadonMessageFilter other;

        NotFilter(GarmadonMessageFilter other) {
            if (other == null) throw new NullPointerException("Null filter is forbidden");
            this.other = other;
        }

        @Override
        public boolean accepts(int type) {
            return !other.accepts(type);
        }

        @Override
        public boolean accepts(int type, EventHeaderProtos.Header header) {
            return !other.accepts(type, header);
        }

    }

    class TypeFilter extends Junction.AbstractBase implements GarmadonMessageFilter {

        private final int type;

        TypeFilter(int type) {
            this.type = type;
        }

        @Override
        public boolean accepts(int type) {
            return this.type == type;
        }

        @Override
        public boolean accepts(int type, EventHeaderProtos.Header header) {
            return this.accepts(type);
        }
    }

    abstract class HeaderFilter extends Junction.AbstractBase implements GarmadonMessageFilter {

        static class TagFilter extends HeaderFilter {

            private final String tag;

            TagFilter(Header.Tag tag) {
                if (tag == null) throw new NullPointerException("Null tag is forbidden");
                this.tag = tag.toString();
            }

            @Override
            public boolean accepts(EventHeaderProtos.Header header) {
                return header.getTagsList().contains(tag);
            }

        }

        static class ContainerFilter extends HeaderFilter {

            private final String id;

            ContainerFilter(String id) {
                if (id == null) throw new NullPointerException("Null id is forbidden");
                this.id = id;
            }

            @Override
            public boolean accepts(EventHeaderProtos.Header header) {
                return id.equals(header.getContainerId());
            }
        }

        static class FrameworkFilter extends HeaderFilter {

            private final String framework;

            FrameworkFilter(String framework) {
                if (framework == null) throw new NullPointerException("Null framework is forbidden");
                this.framework = framework;
            }

            @Override
            public boolean accepts(EventHeaderProtos.Header header) {
                return framework.equals(header.getFramework());
            }
        }

        @Override
        public boolean accepts(int type) {
            return true;
        }

        @Override
        public boolean accepts(int type, EventHeaderProtos.Header header) {
            return accepts(header);
        }

        protected abstract boolean accepts(EventHeaderProtos.Header header);
    }

}
