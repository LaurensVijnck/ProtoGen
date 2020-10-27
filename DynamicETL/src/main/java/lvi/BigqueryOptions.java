// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: protos/bigquery_options.proto

package lvi;

public final class BigqueryOptions {
  private BigqueryOptions() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
    registry.add(BigqueryOptions.tableRoot);
    registry.add(BigqueryOptions.required);
    registry.add(BigqueryOptions.batchAttribute);
    registry.add(BigqueryOptions.description);
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public static final int TABLE_ROOT_FIELD_NUMBER = 51234;
  /**
   * <pre>
   * Which number to pick anyway?
   * </pre>
   *
   * <code>extend .google.protobuf.MessageOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.MessageOptions,
      Boolean> tableRoot = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        Boolean.class,
        null);
  public static final int REQUIRED_FIELD_NUMBER = 1010;
  /**
   * <pre>
   * Which number to pick anyway?
   * </pre>
   *
   * <code>extend .google.protobuf.FieldOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.FieldOptions,
      Boolean> required = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        Boolean.class,
        null);
  public static final int BATCH_ATTRIBUTE_FIELD_NUMBER = 1011;
  /**
   * <code>extend .google.protobuf.FieldOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.FieldOptions,
      Boolean> batchAttribute = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        Boolean.class,
        null);
  public static final int DESCRIPTION_FIELD_NUMBER = 1012;
  /**
   * <code>extend .google.protobuf.FieldOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.FieldOptions,
      String> description = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        String.class,
        null);

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\035protos/bigquery_options.proto\022\003lvi\032 go" +
      "ogle/protobuf/descriptor.proto:5\n\ntable_" +
      "root\022\037.google.protobuf.MessageOptions\030\242\220" +
      "\003 \001(\010:0\n\010required\022\035.google.protobuf.Fiel" +
      "dOptions\030\362\007 \001(\010:7\n\017batch_attribute\022\035.goo" +
      "gle.protobuf.FieldOptions\030\363\007 \001(\010:3\n\013desc" +
      "ription\022\035.google.protobuf.FieldOptions\030\364" +
      "\007 \001(\tb\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.DescriptorProtos.getDescriptor(),
        });
    tableRoot.internalInit(descriptor.getExtensions().get(0));
    required.internalInit(descriptor.getExtensions().get(1));
    batchAttribute.internalInit(descriptor.getExtensions().get(2));
    description.internalInit(descriptor.getExtensions().get(3));
    com.google.protobuf.DescriptorProtos.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
