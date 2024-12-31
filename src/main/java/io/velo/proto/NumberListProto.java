// Generated by the protocol buffer compiler.  DO NOT EDIT!
// NO CHECKED-IN PROTOBUF GENCODE
// source: io/velo/proto/numbers.proto
// Protobuf Java Version: 4.29.2

package io.velo.proto;

@Generated
public final class NumberListProto {
    private NumberListProto() {
    }

    public static void registerAllExtensions(
            com.google.protobuf.ExtensionRegistryLite registry) {
    }

    public interface NumberListOrBuilder extends
            // @@protoc_insertion_point(interface_extends:io.velo.proto.NumberList)
            com.google.protobuf.MessageLiteOrBuilder {

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @return A list containing the numbers32.
         */
        java.util.List<Integer> getNumbers32List();

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @return The count of numbers32.
         */
        int getNumbers32Count();

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @param index The index of the element to return.
         * @return The numbers32 at the given index.
         */
        int getNumbers32(int index);

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @return A list containing the numbers64.
         */
        java.util.List<Long> getNumbers64List();

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @return The count of numbers64.
         */
        int getNumbers64Count();

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @param index The index of the element to return.
         * @return The numbers64 at the given index.
         */
        long getNumbers64(int index);
    }

    /**
     * Protobuf type {@code io.velo.proto.NumberList}
     */
    public static final class NumberList extends
            com.google.protobuf.GeneratedMessageLite<
                    NumberList, NumberList.Builder> implements
            // @@protoc_insertion_point(message_implements:io.velo.proto.NumberList)
            NumberListOrBuilder {
        private NumberList() {
            numbers32_ = emptyIntList();
            numbers64_ = emptyLongList();
        }

        public static final int NUMBERS32_FIELD_NUMBER = 1;
        private com.google.protobuf.Internal.IntList numbers32_;

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @return A list containing the numbers32.
         */
        @Override
        public java.util.List<Integer>
        getNumbers32List() {
            return numbers32_;
        }

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @return The count of numbers32.
         */
        @Override
        public int getNumbers32Count() {
            return numbers32_.size();
        }

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @param index The index of the element to return.
         * @return The numbers32 at the given index.
         */
        @Override
        public int getNumbers32(int index) {
            return numbers32_.getInt(index);
        }

        private int numbers32MemoizedSerializedSize = -1;

        private void ensureNumbers32IsMutable() {
            com.google.protobuf.Internal.IntList tmp = numbers32_;
            if (!tmp.isModifiable()) {
                numbers32_ =
                        com.google.protobuf.GeneratedMessageLite.mutableCopy(tmp);
            }
        }

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @param index The index to set the value at.
         * @param value The numbers32 to set.
         */
        private void setNumbers32(
                int index, int value) {
            ensureNumbers32IsMutable();
            numbers32_.setInt(index, value);
        }

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @param value The numbers32 to add.
         */
        private void addNumbers32(int value) {
            ensureNumbers32IsMutable();
            numbers32_.addInt(value);
        }

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         *
         * @param values The numbers32 to add.
         */
        private void addAllNumbers32(
                Iterable<? extends Integer> values) {
            ensureNumbers32IsMutable();
            com.google.protobuf.AbstractMessageLite.addAll(
                    values, numbers32_);
        }

        /**
         * <pre>
         * repeated means array，sint32/sint64 use zigzag encoding
         * </pre>
         *
         * <code>repeated sint32 numbers32 = 1;</code>
         */
        private void clearNumbers32() {
            numbers32_ = emptyIntList();
        }

        public static final int NUMBERS64_FIELD_NUMBER = 2;
        private com.google.protobuf.Internal.LongList numbers64_;

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @return A list containing the numbers64.
         */
        @Override
        public java.util.List<Long>
        getNumbers64List() {
            return numbers64_;
        }

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @return The count of numbers64.
         */
        @Override
        public int getNumbers64Count() {
            return numbers64_.size();
        }

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @param index The index of the element to return.
         * @return The numbers64 at the given index.
         */
        @Override
        public long getNumbers64(int index) {
            return numbers64_.getLong(index);
        }

        private int numbers64MemoizedSerializedSize = -1;

        private void ensureNumbers64IsMutable() {
            com.google.protobuf.Internal.LongList tmp = numbers64_;
            if (!tmp.isModifiable()) {
                numbers64_ =
                        com.google.protobuf.GeneratedMessageLite.mutableCopy(tmp);
            }
        }

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @param index The index to set the value at.
         * @param value The numbers64 to set.
         */
        private void setNumbers64(
                int index, long value) {
            ensureNumbers64IsMutable();
            numbers64_.setLong(index, value);
        }

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @param value The numbers64 to add.
         */
        private void addNumbers64(long value) {
            ensureNumbers64IsMutable();
            numbers64_.addLong(value);
        }

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         *
         * @param values The numbers64 to add.
         */
        private void addAllNumbers64(
                Iterable<? extends Long> values) {
            ensureNumbers64IsMutable();
            com.google.protobuf.AbstractMessageLite.addAll(
                    values, numbers64_);
        }

        /**
         * <code>repeated sint64 numbers64 = 2;</code>
         */
        private void clearNumbers64() {
            numbers64_ = emptyLongList();
        }

        public static NumberList parseFrom(
                java.nio.ByteBuffer data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data);
        }

        public static NumberList parseFrom(
                java.nio.ByteBuffer data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data, extensionRegistry);
        }

        public static NumberList parseFrom(
                com.google.protobuf.ByteString data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data);
        }

        public static NumberList parseFrom(
                com.google.protobuf.ByteString data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data, extensionRegistry);
        }

        public static NumberList parseFrom(byte[] data)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data);
        }

        public static NumberList parseFrom(
                byte[] data,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, data, extensionRegistry);
        }

        public static NumberList parseFrom(java.io.InputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input);
        }

        public static NumberList parseFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input, extensionRegistry);
        }

        public static NumberList parseDelimitedFrom(java.io.InputStream input)
                throws java.io.IOException {
            return parseDelimitedFrom(DEFAULT_INSTANCE, input);
        }

        public static NumberList parseDelimitedFrom(
                java.io.InputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return parseDelimitedFrom(DEFAULT_INSTANCE, input, extensionRegistry);
        }

        public static NumberList parseFrom(
                com.google.protobuf.CodedInputStream input)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input);
        }

        public static NumberList parseFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            return com.google.protobuf.GeneratedMessageLite.parseFrom(
                    DEFAULT_INSTANCE, input, extensionRegistry);
        }

        public static Builder newBuilder() {
            return (Builder) DEFAULT_INSTANCE.createBuilder();
        }

        public static Builder newBuilder(NumberList prototype) {
            return DEFAULT_INSTANCE.createBuilder(prototype);
        }

        /**
         * Protobuf type {@code io.velo.proto.NumberList}
         */
        public static final class Builder extends
                com.google.protobuf.GeneratedMessageLite.Builder<
                        NumberList, Builder> implements
                // @@protoc_insertion_point(builder_implements:io.velo.proto.NumberList)
                NumberListOrBuilder {
            // Construct using io.velo.proto.NumberListProto.NumberList.newBuilder()
            private Builder() {
                super(DEFAULT_INSTANCE);
            }


            /**
             * <pre>
             * repeated means array，sint32/sint64 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 numbers32 = 1;</code>
             *
             * @return A list containing the numbers32.
             */
            @Override
            public java.util.List<Integer>
            getNumbers32List() {
                return java.util.Collections.unmodifiableList(
                        instance.getNumbers32List());
            }

            /**
             * <pre>
             * repeated means array，sint32/sint64 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 numbers32 = 1;</code>
             *
             * @return The count of numbers32.
             */
            @Override
            public int getNumbers32Count() {
                return instance.getNumbers32Count();
            }

            /**
             * <pre>
             * repeated means array，sint32/sint64 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 numbers32 = 1;</code>
             *
             * @param index The index of the element to return.
             * @return The numbers32 at the given index.
             */
            @Override
            public int getNumbers32(int index) {
                return instance.getNumbers32(index);
            }

            /**
             * <pre>
             * repeated means array，sint32/sint64 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 numbers32 = 1;</code>
             *
             * @param value The numbers32 to set.
             * @return This builder for chaining.
             */
            public Builder setNumbers32(
                    int index, int value) {
                copyOnWrite();
                instance.setNumbers32(index, value);
                return this;
            }

            /**
             * <pre>
             * repeated means array，sint32/sint64 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 numbers32 = 1;</code>
             *
             * @param value The numbers32 to add.
             * @return This builder for chaining.
             */
            public Builder addNumbers32(int value) {
                copyOnWrite();
                instance.addNumbers32(value);
                return this;
            }

            /**
             * <pre>
             * repeated means array，sint32/sint64 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 numbers32 = 1;</code>
             *
             * @param values The numbers32 to add.
             * @return This builder for chaining.
             */
            public Builder addAllNumbers32(
                    Iterable<? extends Integer> values) {
                copyOnWrite();
                instance.addAllNumbers32(values);
                return this;
            }

            /**
             * <pre>
             * repeated means array，sint32/sint64 use zigzag encoding
             * </pre>
             *
             * <code>repeated sint32 numbers32 = 1;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearNumbers32() {
                copyOnWrite();
                instance.clearNumbers32();
                return this;
            }

            /**
             * <code>repeated sint64 numbers64 = 2;</code>
             *
             * @return A list containing the numbers64.
             */
            @Override
            public java.util.List<Long>
            getNumbers64List() {
                return java.util.Collections.unmodifiableList(
                        instance.getNumbers64List());
            }

            /**
             * <code>repeated sint64 numbers64 = 2;</code>
             *
             * @return The count of numbers64.
             */
            @Override
            public int getNumbers64Count() {
                return instance.getNumbers64Count();
            }

            /**
             * <code>repeated sint64 numbers64 = 2;</code>
             *
             * @param index The index of the element to return.
             * @return The numbers64 at the given index.
             */
            @Override
            public long getNumbers64(int index) {
                return instance.getNumbers64(index);
            }

            /**
             * <code>repeated sint64 numbers64 = 2;</code>
             *
             * @param value The numbers64 to set.
             * @return This builder for chaining.
             */
            public Builder setNumbers64(
                    int index, long value) {
                copyOnWrite();
                instance.setNumbers64(index, value);
                return this;
            }

            /**
             * <code>repeated sint64 numbers64 = 2;</code>
             *
             * @param value The numbers64 to add.
             * @return This builder for chaining.
             */
            public Builder addNumbers64(long value) {
                copyOnWrite();
                instance.addNumbers64(value);
                return this;
            }

            /**
             * <code>repeated sint64 numbers64 = 2;</code>
             *
             * @param values The numbers64 to add.
             * @return This builder for chaining.
             */
            public Builder addAllNumbers64(
                    Iterable<? extends Long> values) {
                copyOnWrite();
                instance.addAllNumbers64(values);
                return this;
            }

            /**
             * <code>repeated sint64 numbers64 = 2;</code>
             *
             * @return This builder for chaining.
             */
            public Builder clearNumbers64() {
                copyOnWrite();
                instance.clearNumbers64();
                return this;
            }

            // @@protoc_insertion_point(builder_scope:io.velo.proto.NumberList)
        }

        @Override
        @SuppressWarnings({"unchecked", "fallthrough"})
        protected final Object dynamicMethod(
                MethodToInvoke method,
                Object arg0, Object arg1) {
            switch (method) {
                case NEW_MUTABLE_INSTANCE: {
                    return new NumberList();
                }
                case NEW_BUILDER: {
                    return new Builder();
                }
                case BUILD_MESSAGE_INFO: {
                    Object[] objects = new Object[]{
                            "numbers32_",
                            "numbers64_",
                    };
                    String info =
                            "\u0000\u0002\u0000\u0000\u0001\u0002\u0002\u0000\u0002\u0000\u0001/\u00020";
                    return newMessageInfo(DEFAULT_INSTANCE, info, objects);
                }
                // fall through
                case GET_DEFAULT_INSTANCE: {
                    return DEFAULT_INSTANCE;
                }
                case GET_PARSER: {
                    com.google.protobuf.Parser<NumberList> parser = PARSER;
                    if (parser == null) {
                        synchronized (NumberList.class) {
                            parser = PARSER;
                            if (parser == null) {
                                parser =
                                        new DefaultInstanceBasedParser<NumberList>(
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


        // @@protoc_insertion_point(class_scope:io.velo.proto.NumberList)
        private static final NumberList DEFAULT_INSTANCE;

        static {
            NumberList defaultInstance = new NumberList();
            // New instances are implicitly immutable so no need to make
            // immutable.
            DEFAULT_INSTANCE = defaultInstance;
            com.google.protobuf.GeneratedMessageLite.registerDefaultInstance(
                    NumberList.class, defaultInstance);
        }

        public static NumberList getDefaultInstance() {
            return DEFAULT_INSTANCE;
        }

        private static volatile com.google.protobuf.Parser<NumberList> PARSER;

        public static com.google.protobuf.Parser<NumberList> parser() {
            return DEFAULT_INSTANCE.getParserForType();
        }
    }


    static {
    }

    // @@protoc_insertion_point(outer_class_scope)
}
