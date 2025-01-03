// Generated by the protocol buffer compiler.  DO NOT EDIT!
// NO CHECKED-IN PROTOBUF GENCODE
// source: io/velo/proto/counts.proto
// Protobuf Java Version: 4.29.2

package io.velo.proto;

@Generated
public final class CountListProto {
    private CountListProto() {
    }

    public static void registerAllExtensions(
            com.google.protobuf.ExtensionRegistryLite registry) {
    }

    public interface CountListOrBuilder extends
            // @@protoc_insertion_point(interface_extends:io.velo.proto.CountList)
            com.google.protobuf.MessageLiteOrBuilder {

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @return A list containing the keyHash32AndCount.
         */
        java.util.List<Integer> getKeyHash32AndCountList();

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @return The count of keyHash32AndCount.
         */
        int getKeyHash32AndCountCount();

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @param index The index of the element to return.
         * @return The keyHash32AndCount at the given index.
         */
        int getKeyHash32AndCount(int index);
    }

    /**
     * Protobuf type {@code io.velo.proto.CountList}
     */
    public static final class CountList extends
            com.google.protobuf.GeneratedMessageLite<
                    CountList, CountList.Builder> implements
            // @@protoc_insertion_point(message_implements:io.velo.proto.CountList)
            CountListOrBuilder {
        private CountList() {
            keyHash32AndCount_ = emptyIntList();
        }

        public static final int KEYHASH32ANDCOUNT_FIELD_NUMBER = 1;
        private com.google.protobuf.Internal.IntList keyHash32AndCount_;

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @return A list containing the keyHash32AndCount.
         */
        @Override
        public java.util.List<Integer>
        getKeyHash32AndCountList() {
            return keyHash32AndCount_;
        }

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @return The count of keyHash32AndCount.
         */
        @Override
        public int getKeyHash32AndCountCount() {
            return keyHash32AndCount_.size();
        }

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @param index The index of the element to return.
         * @return The keyHash32AndCount at the given index.
         */
        @Override
        public int getKeyHash32AndCount(int index) {
            return keyHash32AndCount_.getInt(index);
        }

        private int keyHash32AndCountMemoizedSerializedSize = -1;

        private void ensureKeyHash32AndCountIsMutable() {
            com.google.protobuf.Internal.IntList tmp = keyHash32AndCount_;
            if (!tmp.isModifiable()) {
                keyHash32AndCount_ =
                        com.google.protobuf.GeneratedMessageLite.mutableCopy(tmp);
            }
        }

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @param index The index to set the value at.
         * @param value The keyHash32AndCount to set.
         */
        private void setKeyHash32AndCount(
                int index, int value) {
            ensureKeyHash32AndCountIsMutable();
            keyHash32AndCount_.setInt(index, value);
        }

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @param value The keyHash32AndCount to add.
         */
        private void addKeyHash32AndCount(int value) {
            ensureKeyHash32AndCountIsMutable();
            keyHash32AndCount_.addInt(value);
        }

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         *
         * @param values The keyHash32AndCount to add.
         */
        private void addAllKeyHash32AndCount(
                Iterable<? extends Integer> values) {
            ensureKeyHash32AndCountIsMutable();
            com.google.protobuf.AbstractMessageLite.addAll(
                    values, keyHash32AndCount_);
        }

        /**
         * <pre>
         * repeated means array，sint32 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 keyHash32AndCount = 1;</code>
         */
        private void clearKeyHash32AndCount() {
            keyHash32AndCount_ = emptyIntList();
        }

        public static CountList parseFrom(
                java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data);
        }

        public static CountList parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data, extensionRegistry);
        }

        public static CountList parseFrom(
                com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data);
        }

        public static CountList parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data, extensionRegistry);
        }

        public static CountList parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data);
        }

        public static CountList parseFrom(
                byte[] data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data, extensionRegistry);
        }

        public static CountList parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input);
        }

        public static CountList parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input, extensionRegistry);
        }

        public static CountList parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return parseDelimitedFrom(DEFAULT_INSTANCE, input);
        }

        public static CountList parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return parseDelimitedFrom(DEFAULT_INSTANCE, input, extensionRegistry);
        }

        public static CountList parseFrom(
                com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input);
        }

        public static CountList parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input, extensionRegistry);
        }

        public static Builder newBuilder() {
            return (Builder) DEFAULT_INSTANCE.createBuilder();
        }

        public static Builder newBuilder(CountList prototype) {
            return DEFAULT_INSTANCE.createBuilder(prototype);
        }

        /**
         * Protobuf type {@code io.velo.proto.CountList}
         */
        public static final class Builder extends
                com.google.protobuf.GeneratedMessageLite.Builder<
                        CountList, Builder> implements
                // @@protoc_insertion_point(builder_implements:io.velo.proto.CountList)
                CountListOrBuilder {
            // Construct using io.velo.proto.CountListProto.CountList.newBuilder()
            private Builder() {
                super(DEFAULT_INSTANCE);
            }


            /**
             * <pre>
             * repeated means array，sint32 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 keyHash32AndCount = 1;</code>
             *
             * @return A list containing the keyHash32AndCount.
             */
            @Override
            public java.util.List<Integer>
            getKeyHash32AndCountList() {
                return java.util.Collections.unmodifiableList(
                        instance.getKeyHash32AndCountList());
            }

            /**
             * <pre>
             * repeated means array，sint32 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 keyHash32AndCount = 1;</code>
             *
             * @return The count of keyHash32AndCount.
             */
            @Override
            public int getKeyHash32AndCountCount() {
                return instance.getKeyHash32AndCountCount();
            }

            /**
             * <pre>
             * repeated means array，sint32 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 keyHash32AndCount = 1;</code>
             *
             * @param index The index of the element to return.
             * @return The keyHash32AndCount at the given index.
             */
            @Override
            public int getKeyHash32AndCount(int index) {
                return instance.getKeyHash32AndCount(index);
            }

            /**
             * <pre>
             * repeated means array，sint32 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 keyHash32AndCount = 1;</code>
             *
             * @param value The keyHash32AndCount to set.
             * @return This builder for chaining.
             */
            public Builder setKeyHash32AndCount(
                    int index, int value) {
                copyOnWrite();
                instance.setKeyHash32AndCount(index, value);
                return this;
            }

            /**
             * <pre>
             * repeated means array，sint32 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 keyHash32AndCount = 1;</code>
             *
             * @param value The keyHash32AndCount to add.
             * @return This builder for chaining.
             */
            public Builder addKeyHash32AndCount(int value) {
                copyOnWrite();
                instance.addKeyHash32AndCount(value);
                return this;
            }

            /**
             * <pre>
             * repeated means array，sint32 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 keyHash32AndCount = 1;</code>
             *
             * @param values The keyHash32AndCount to add.
             * @return This builder for chaining.
             */
            public Builder addAllKeyHash32AndCount(
                    Iterable<? extends Integer> values) {
                copyOnWrite();
                instance.addAllKeyHash32AndCount(values);
                return this;
            }

            /**
             * <pre>
             * repeated means array，sint32 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 keyHash32AndCount = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearKeyHash32AndCount() {
                copyOnWrite();
                instance.clearKeyHash32AndCount();
                return this;
            }

            // @@protoc_insertion_point(builder_scope:io.velo.proto.CountList)
        }

        @Override
        @SuppressWarnings({"unchecked", "fallthrough"})
        protected final Object dynamicMethod(
                MethodToInvoke method,
                Object arg0, Object arg1) {
            switch (method) {
                case NEW_MUTABLE_INSTANCE: {
                    return new CountList();
                }
                case NEW_BUILDER: {
                    return new Builder();
                }
                case BUILD_MESSAGE_INFO: {
                    Object[] objects = new Object[]{
                            "keyHash32AndCount_",
                    };
                    String info =
                            "\u0000\u0001\u0000\u0000\u0001\u0001\u0001\u0000\u0001\u0000\u0001/";
                    return newMessageInfo(DEFAULT_INSTANCE, info, objects);
                }
                // fall through
                case GET_DEFAULT_INSTANCE: {
                    return DEFAULT_INSTANCE;
                }
                case GET_PARSER: {
                    com.google.protobuf.Parser<CountList> parser = PARSER;
                    if (parser == null) {
                        synchronized (CountList.class) {
                            parser = PARSER;
                            if (parser == null) {
                                parser =
                                        new DefaultInstanceBasedParser<CountList>(
                                                DEFAULT_INSTANCE);
                                PARSER = parser;
                            }
                        }
                    }
                    return parser;
                }
                case GET_MEMOIZED_IS_INITIALIZED: {
                    return (byte) 1;
                }
                case SET_MEMOIZED_IS_INITIALIZED: {
                    return null;
                }
            }
            throw new UnsupportedOperationException();
        }


        // @@protoc_insertion_point(class_scope:io.velo.proto.CountList)
        private static final CountList DEFAULT_INSTANCE;

        static {
            CountList defaultInstance = new CountList();
            // New instances are implicitly immutable so no need to make
            // immutable.
            DEFAULT_INSTANCE = defaultInstance;
            com.google.protobuf.GeneratedMessageLite.registerDefaultInstance(
                    CountList.class, defaultInstance);
        }

        public static CountList getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static volatile com.google.protobuf.Parser<CountList> PARSER;

        public static com.google.protobuf.Parser<CountList> parser() {
            return DEFAULT_INSTANCE.getParserForType();
        }
    }


    static {
    }

    // @@protoc_insertion_point(outer_class_scope)
}