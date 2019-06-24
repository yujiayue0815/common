package com.yujiayue.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.util.StringJoiner;

public class FamilyParam {
    /**
     * 列族名称
     */
    private String family;

    /**
     * 设置块缓存
     */
    private Boolean blockCacheEnabled;
    /**
     * 缓存块大小
     */
    private Integer blockSize;

    /**
     * 压缩方式
     */
    private Compression.Algorithm algorithm;
    /**
     * 最大版本
     */
    private Integer maxVersionNum;
    /**
     * 最小版本
     */
    private Integer minVersionNum;

    public FamilyParam(String family, boolean blockCacheEnabled, Integer blockSize, Compression.Algorithm algorithm, Integer maxVersionNum, Integer minVersionNum) {
        this.family = family;
        this.blockCacheEnabled = blockCacheEnabled;
        this.blockSize = blockSize;
        this.algorithm = algorithm;
        this.maxVersionNum = maxVersionNum;
        this.minVersionNum = minVersionNum;
    }

    public FamilyParam() {
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public boolean isBlockCacheEnabled() {
        return blockCacheEnabled;
    }

    public void setBlockCacheEnabled(boolean blockCacheEnabled) {
        this.blockCacheEnabled = blockCacheEnabled;
    }

    public Integer getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
    }

    public Compression.Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Compression.Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public Integer getMaxVersionNum() {
        return maxVersionNum;
    }

    public void setMaxVersionNum(Integer maxVersionNum) {
        this.maxVersionNum = maxVersionNum;
    }

    public Integer getMinVersionNum() {
        return minVersionNum;
    }

    public void setMinVersionNum(Integer minVersionNum) {
        this.minVersionNum = minVersionNum;
    }

    /**
     * 自动配置
     *
     * @param columnDescriptor
     * @return
     */
    public HColumnDescriptor autoConfig(HColumnDescriptor columnDescriptor) {
        if (this.algorithm != null) columnDescriptor.setCompressionType(this.algorithm);
        if (this.blockCacheEnabled != null) columnDescriptor.setBlockCacheEnabled(this.blockCacheEnabled);
        if (this.blockSize != null) columnDescriptor.setBlocksize(this.blockSize);
        if (this.maxVersionNum != null) columnDescriptor.setMaxVersions(this.maxVersionNum);
        if (this.minVersionNum != null) columnDescriptor.setMinVersions(this.minVersionNum);
        return columnDescriptor;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FamilyParam.class.getSimpleName() + "[", "]")
                .add("family='" + family + "'")
                .add("blockCacheEnabled=" + blockCacheEnabled)
                .add("blockSize=" + blockSize)
                .add("algorithm=" + algorithm)
                .add("maxVersionNum=" + maxVersionNum)
                .add("minVersionNum=" + minVersionNum)
                .toString();
    }
}
