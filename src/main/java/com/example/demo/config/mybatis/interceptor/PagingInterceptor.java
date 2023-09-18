package com.example.demo.config.mybatis.interceptor;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import com.example.demo.config.mybatis.model.PagableResponse;
import com.example.demo.config.mybatis.model.PageInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

@Slf4j
//@Intercepts({@Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class})})
@Intercepts({
        @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class }),
        //@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class }),
        //@Signature(type = Executor.class, method = "queryCursor", args = { MappedStatement.class, Object.class, RowBounds.class }),
        //@Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
})
public class PagingInterceptor implements Interceptor {

    private static String COUNT_ID_SUFFIX = "-PagingCount";


    private String getMapperSql(Invocation invocation) throws Exception
    {
        MappedStatement ms = null;
        Object parameter = null;
        BoundSql boundSql = null;

        for (Object arg : invocation.getArgs()) {
            if (arg instanceof MappedStatement) {
                ms = (MappedStatement) arg;
            } else if (arg instanceof RowBounds || arg instanceof ResultHandler || arg instanceof CacheKey) {
                // skip
            } else if (arg instanceof BoundSql) {
                boundSql = (BoundSql) arg;
            } else if (arg != null) {
                parameter = arg;
            }
        }

        if (boundSql == null && ms != null) {
            boundSql = ms.getBoundSql(parameter);
        }

        if (boundSql == null) throw new Exception("boundSql is empty!");

        StringBuilder sql = new StringBuilder(boundSql.getSql().replaceAll("(?m)^[ \t]*\r?\n", ""));

        if (parameter != null) {
            for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
                String property = parameterMapping.getProperty();
                Object value = "<NotFound>";

                if (boundSql.hasAdditionalParameter(property)) {
                    value = boundSql.getAdditionalParameter(property);
                } else if (ClassUtils.isPrimitiveOrWrapper(parameter.getClass())) {
                    value = parameter;
                } else if (parameter instanceof Map) {
                    value = ((Map<?, ?>) parameter).get(property);
                } else if (parameter instanceof String) {
                    value = (String) parameter;
                } else {
                    PropertyDescriptor[] propertyDescriptors = Introspector.getBeanInfo(parameter.getClass()).getPropertyDescriptors();

                    for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                        String name = propertyDescriptor.getName();

                        if (property.equals(name) || property.equals(StringUtils.capitalize(name))) {
                            value = propertyDescriptor.getReadMethod().invoke(parameter);
                            break;
                        }
                    }
                }

                int start = sql.indexOf("?");
                int end = start + 1;

                if (value == null) {
                    sql.replace(start, end, "NULL");
                } else if (value instanceof String) {
                    sql.replace(start, end, "'" + value.toString() + "'");
                } else {
                    sql.replace(start, end, value.toString());
                }
            }
        }

        return sql.toString();
    }

    private MappedStatement getMappedStatement(MappedStatement ms, String id, SqlSource sqlSource, List<ResultMap> resultMaps)
    {
        return new MappedStatement.Builder(ms.getConfiguration(), id, sqlSource, ms.getSqlCommandType())
                .resource(ms.getResource())
                .parameterMap(ms.getParameterMap())
                .resultMaps(resultMaps)
                .fetchSize(ms.getFetchSize())
                .timeout(ms.getTimeout())
                .statementType(ms.getStatementType())
                .resultSetType(ms.getResultSetType())
                .cache(ms.getCache())
                .flushCacheRequired(ms.isFlushCacheRequired())
                .useCache(true)
                .resultOrdered(ms.isResultOrdered())
                .keyGenerator(ms.getKeyGenerator())
                .keyColumn(ms.getKeyColumns() != null ? String.join(",", ms.getKeyColumns()) : null)
                .keyProperty(ms.getKeyProperties() != null ? String.join(",", ms.getKeyProperties()): null)
                .databaseId(ms.getDatabaseId())
                .lang(ms.getLang())
                .resultSets(ms.getResultSets() != null ? String.join(",", ms.getResultSets()): null)
                .build();
    }

    private List<ResultMap> createCountResultMaps(MappedStatement ms) {
        List<ResultMap> countResultMaps = new ArrayList<>();

        ResultMap countResultMap =
                new ResultMap.Builder(ms.getConfiguration(), ms.getId() + COUNT_ID_SUFFIX, Long.class, new ArrayList<>())
                        .build();
        countResultMaps.add(countResultMap);

        return countResultMaps;
    }

    private MappedStatement createPageMappedStatement(Invocation invocation, String mapperSql, PageInfo pageInfo) {
        MappedStatement ms = (MappedStatement) invocation.getArgs()[0];

        int offset = (pageInfo.getPage() - 1) * pageInfo.getSize();
        int limit = pageInfo.getSize();

        String pagingSql = mapperSql + " LIMIT " + limit + " OFFSET " + offset;
        log.debug("PAGING SQL = \n{}", pagingSql);

        StaticSqlSource sqlSource = new StaticSqlSource(ms.getConfiguration(), pagingSql);
        return getMappedStatement(ms, ms.getId(), sqlSource, ms.getResultMaps());
    }

    private MappedStatement createCountMappedStatement(Invocation invocation, String mapperSql) {
        MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
        List<ResultMap> countResultMaps = new ArrayList<>();

        ResultMap countResultMap =
                new ResultMap.Builder(ms.getConfiguration(), ms.getId() + COUNT_ID_SUFFIX, Long.class, new ArrayList<>())
                        .build();
        countResultMaps.add(countResultMap);

        String countSql = mapperSql.replace("*", "count(*)");
        log.debug("COUNT SQL = \n{}", countSql);

        StaticSqlSource sqlSource = new StaticSqlSource(ms.getConfiguration(), countSql);
        return getMappedStatement(ms, ms.getId() + COUNT_ID_SUFFIX, sqlSource, countResultMaps);
    }

    private PagableResponse<Object> createPagableResponse(List<Object> list, PageInfo pageInfo) {
        PagableResponse<Object> pagableResponse = new PagableResponse<>();
        pagableResponse.setList(list);
        pagableResponse.getPageInfo().setPage(pageInfo.getPage());
        pagableResponse.getPageInfo().setSize(pageInfo.getSize());
        pagableResponse.getPageInfo().setTotalCount(pageInfo.getTotalCount());

        return pagableResponse;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object intercept(Invocation invocation) throws Throwable {

        PageInfo pageInfo = null;
        for (Object arg : invocation.getArgs()) {
            if (arg instanceof PageInfo) pageInfo = (PageInfo) arg;
        }

        if (pageInfo == null) return invocation.proceed();

        log.debug("page = {}, size = {}", pageInfo.getPage(), pageInfo.getSize());
        if (pageInfo.getPage() == null || pageInfo.getSize() == null) return invocation.proceed();
        if (pageInfo.getPage() <= 0 || pageInfo.getSize() <= 0) return invocation.proceed();

        String mapperSql = getMapperSql(invocation);

        // LIST 援ы븯湲�
        invocation.getArgs()[0] = createPageMappedStatement(invocation, mapperSql, pageInfo);
        List<Object> list = (List<Object>) invocation.proceed();
        log.debug("sql = {}, selected rows = {}", ((MappedStatement)(invocation.getArgs()[0])).getId(), list.size());

        // COUNT 援ы븯湲�
        invocation.getArgs()[0] = createCountMappedStatement(invocation, mapperSql);
        List<Long> totalCount = (List<Long>) invocation.proceed();
        pageInfo.setTotalCount((Long) totalCount.get(0));
        log.debug("sql = {}, total count = {}", ((MappedStatement)(invocation.getArgs()[0])).getId(), totalCount.get(0));

        return createPagableResponse(list, pageInfo);
    }
}