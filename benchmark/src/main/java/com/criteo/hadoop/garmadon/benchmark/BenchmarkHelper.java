package com.criteo.hadoop.garmadon.benchmark;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;

import static org.objectweb.asm.Opcodes.*;

public class BenchmarkHelper {

    public static Class createClassDefinition(String className){
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        cw.visit(
                V1_8,
                ACC_PUBLIC,
                "com/criteo/hadoop/garmadon/benchmark/" + className,
                null,
                "java/lang/Object",
                null
        );
        //create a default constructor
        MethodVisitor cv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        cv.visitVarInsn(ALOAD, 0);
        cv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        cv.visitInsn(RETURN);
        cv.visitMaxs(1, 1);
        cw.visitEnd();

        return loadClass(cw.toByteArray());
    }

    private static Class loadClass(byte[] b) {
        //override classDefine (as it is protected) and define the class.
        Class clazz = null;
        try {
            ClassLoader loader = ClassLoader.getSystemClassLoader();
            Class cls = Class.forName("java.lang.ClassLoader");
            java.lang.reflect.Method method =
                    cls.getDeclaredMethod("defineClass", String.class, byte[].class, int.class, int.class);

            // protected method invocaton
            method.setAccessible(true);
            try {
                Object[] args = new Object[] { null, b, 0, b.length};
                clazz = (Class) method.invoke(loader, args);
            } finally {
                method.setAccessible(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return clazz;
    }

}
