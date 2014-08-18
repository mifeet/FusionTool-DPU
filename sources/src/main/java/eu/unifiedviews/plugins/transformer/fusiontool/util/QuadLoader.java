package eu.unifiedviews.plugins.transformer.fusiontool.util;

import cz.cuni.mff.odcleanstore.shared.util.LimitedURIListBuilder;
import eu.unifiedviews.dataunit.DataUnitException;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigConstants;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuErrorCodes;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuException;
import eu.unifiedviews.plugins.transformer.fusiontool.urimapping.AlternativeURINavigator;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Loads statements having a given URI as their subject, taking into consideration
 * given owl:sameAs alternatives.
 * @author Jan Michelfeit
 */
public class QuadLoader {
    private static final Logger LOG = LoggerFactory.getLogger(QuadLoader.class);

    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();

    /**
     * Maximum number of values in a generated argument for the "?var IN (...)" SPARQL construct .
     */
    protected static final int MAX_QUERY_LIST_LENGTH = ConfigConstants.MAX_QUERY_LIST_LENGTH;

    /**
     * SPARQL query that gets all quads having the given uri as their subject.
     * Quads are loaded from named graphs optionally limited by named graph restriction pattern.
     * This query is to be used when there are no owl:sameAs alternatives for the given URI.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) searched uri
     * (3) list of graph names (e.g. "<uri1>,<uri2>,<uri3>")
     */
    private static final String QUADS_QUERY_SIMPLE = "%1$s" 
            + "\n SELECT DISTINCT (<%2$s> AS ?s) ?p ?o ?g"
            + "\n WHERE { " 
            + "\n   graph ?g { <%2$s> ?p ?o }"
            + "\n   FILTER (?g IN (%3$s))" // FIXME
            + "\n }";

    /**
     * SPARQL query that gets all quads having one of the given URIs as their subject.
     * Quads are loaded from named graphs optionally limited by named graph restriction pattern.
     * This query is to be used when there are multiple owl:sameAs alternatives.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) list of searched URIs (e.g. "<uri1>,<uri2>,<uri3>")
     * (3) list of graph names (e.g. "<uri1>,<uri2>,<uri3>")
     */
    private static final String QUADS_QUERY_ALTERNATIVE = "%1$s"
            + "\n SELECT DISTINCT ?s ?p ?o ?g"
            + "\n WHERE {"
            + "\n   graph ?g {?s ?p ?o}"
            + "\n   FILTER (?g IN (%3$s))" // FIXME
            + "\n   FILTER (?s IN (%2$s))" // TODO: replace by a large union for efficiency?
            + "\n }";

    private static final String SUBJECT_VAR = "s";
    private static final String PROPERTY_VAR = "p";
    private static final String OBJECT_VAR = "o";
    private static final String GRAPH_VAR = "g";

    private final AlternativeURINavigator alternativeURINavigator;
    private final List<RDFDataUnit> rdfDataUnits;
    private final PrefixDeclBuilder nsPrefixes;

    /**
     * Creates a new instance.
     * @param rdfDataUnits input data
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     * @param nsPrefixes holder for namespace prefixes
     */
    public QuadLoader(
            List<RDFDataUnit> rdfDataUnits, AlternativeURINavigator alternativeURINavigator, PrefixDeclBuilder nsPrefixes) {
        
        this.rdfDataUnits = rdfDataUnits;
        this.alternativeURINavigator = alternativeURINavigator;
        this.nsPrefixes = nsPrefixes;
    }

    /**
     * Adds quads having the given uri or one of its owl:sameAs alternatives as their subject to quadCollection.
     * @param uri searched subject URI
     * @return selected data from the RDF input data
     * @throws FusionToolDpuException query error
     */
    public Collection<Statement> loadQuadsForURI(String uri) throws FusionToolDpuException {
        long startTime = 0;
        if (LOG.isTraceEnabled()) {
            startTime = System.currentTimeMillis();
        }

        uri = uri.trim();
        List<String> alternativeURIs = alternativeURINavigator.listAlternativeURIs(uri);
        Collection<Statement> result = new ArrayList<Statement>();

        try {
            for (RDFDataUnit rdfData : this.rdfDataUnits) {
                String graphsList = getGraphsList(rdfData);
                if (alternativeURIs.size() <= 1) {
                    String query = String.format(QUADS_QUERY_SIMPLE, nsPrefixes.getPrefixDecl(), uri, graphsList);
                    addQuadsFromQuery(rdfData, query, result);
                } else {
                    Iterable<CharSequence> limitedURIListBuilder =
                            new LimitedURIListBuilder(alternativeURIs, MAX_QUERY_LIST_LENGTH);
                    for (CharSequence uriList : limitedURIListBuilder) {
                        String query = String.format(QUADS_QUERY_ALTERNATIVE, nsPrefixes.getPrefixDecl(), uriList, graphsList);
                        addQuadsFromQuery(rdfData, query, result);
                    }
                }
            }
        } catch (QueryEvaluationException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.QUERY_QUADS, "Query evaluation error when loading quads for URI " + uri, e);
        } catch (DataUnitException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.QUERY_QUADS, "DataUnit error when loading data for URI " + uri, e);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Loaded {} quads for URI {} in {} ms", 
                    new Object[] { result.size(), uri, System.currentTimeMillis() - startTime });
        }

        return result;
    }

    private String getGraphsList(RDFDataUnit rdfData) throws DataUnitException {
        RDFDataUnit.Iteration it = rdfData.getIteration();
        StringBuilder stringBuilder = new StringBuilder();
        if (it.hasNext()) {
            stringBuilder.append('<');
            stringBuilder.append(it.next().getDataGraphURI().stringValue());
            stringBuilder.append('>');
            while (it.hasNext()) {
                stringBuilder.append(',');
                stringBuilder.append('<');
                stringBuilder.append(it.next().getDataGraphURI().stringValue());
                stringBuilder.append('>');
            }
        }
        return stringBuilder.toString();
    }

    /**
     * Execute the given SPARQL SELECT and constructs a collection of quads from the result.
     * @param rdfData input RDF data
     * @param sparqlQuery a SPARQL SELECT query with four variables in the result: named graph, subject, property
     * @param quads collection where the retrieved quads are added
     * @throws QueryEvaluationException query error
     */
    private void addQuadsFromQuery(RDFDataUnit rdfData, String sparqlQuery, Collection<Statement> quads)
            throws QueryEvaluationException {

        TupleQueryResult queryResult = null;
        try {
            queryResult = rdfData.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate();
            while (queryResult.hasNext()) {
                
                BindingSet bindings = queryResult.next();
                Statement quad = VALUE_FACTORY.createStatement(
                        (Resource) bindings.getValue(SUBJECT_VAR),
                        (URI) bindings.getValue(PROPERTY_VAR),
                        bindings.getValue(OBJECT_VAR),
                        (Resource) bindings.getValue(GRAPH_VAR));
                quads.add(quad);
            }
        } catch (OpenRDFException | DataUnitException e) {
            throw new QueryEvaluationException(e);
        } finally {
            if (queryResult != null) {
                queryResult.close();
            }
        }
    }
}
