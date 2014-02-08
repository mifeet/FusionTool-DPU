package cz.cuni.mff.xrg.odcs.dpu.fusiontool.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

import cz.cuni.mff.odcleanstore.shared.util.LimitedURIListBuilder;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.ConfigConstants;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.FusionToolDpuErrorCodes;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.FusionToolDpuException;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.urimapping.AlternativeURINavigator;
import cz.cuni.mff.xrg.odcs.rdf.exceptions.InvalidQueryException;
import cz.cuni.mff.xrg.odcs.rdf.interfaces.RDFDataUnit;

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
     */
    private static final String QUADS_QUERY_SIMPLE = "%1$s" 
            + "\n SELECT DISTINCT (<%2$s> AS ?s) ?p ?o"
            + "\n WHERE { " 
            + "\n   <%2$s> ?p ?o "
            + "\n }";

    /**
     * SPARQL query that gets all quads having one of the given URIs as their subject.
     * Quads are loaded from named graphs optionally limited by named graph restriction pattern.
     * This query is to be used when there are multiple owl:sameAs alternatives.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) list of searched URIs (e.g. "<uri1>,<uri2>,<uri3>")
     */
    private static final String QUADS_QUERY_ALTERNATIVE = "%1$s"
            + "\n SELECT DISTINCT ?s ?p ?o"
            + "\n WHERE {"
            + "\n   ?s ?p ?o"
            + "\n   FILTER (?s IN (%2$s))" // TODO: replace by a large union for efficiency?
            + "\n }";

    private static final String SUBJECT_VAR = "s";
    private static final String PROPERTY_VAR = "p";
    private static final String OBJECT_VAR = "o";

    private final AlternativeURINavigator alternativeURINavigator;
    private final RDFDataUnit rdfData;
    private final PrefixDeclBuilder nsPrefixes;

    /**
     * Creates a new instance.
     * @param rdfData input data
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     * @param nsPrefixes holder for namespace prefixes
     */
    public QuadLoader(RDFDataUnit rdfData, AlternativeURINavigator alternativeURINavigator, PrefixDeclBuilder nsPrefixes) {
        this.rdfData = rdfData;
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
            if (alternativeURIs.size() <= 1) {
                String query = String.format(QUADS_QUERY_SIMPLE, nsPrefixes.getPrefixDecl(), uri);
                addQuadsFromQuery(query, result);
            } else {
                Iterable<CharSequence> limitedURIListBuilder = new LimitedURIListBuilder(alternativeURIs, MAX_QUERY_LIST_LENGTH);
                for (CharSequence uriList : limitedURIListBuilder) {
                    String query = String.format(QUADS_QUERY_ALTERNATIVE, nsPrefixes.getPrefixDecl(), uriList);
                    addQuadsFromQuery(query, result);
                }
            }
        } catch (InvalidQueryException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.QUERY_QUADS, "Invalid query when loading quads for URI " + uri, e);
        } catch (QueryEvaluationException e) {
            throw new FusionToolDpuException(
                    FusionToolDpuErrorCodes.QUERY_QUADS, "Query evaluation error when loading quads for URI " + uri, e);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Loaded {} quads for URI {} in {} ms", 
                    new Object[] { result.size(), uri, System.currentTimeMillis() - startTime });
        }

        return result;
    }

    /**
     * Execute the given SPARQL SELECT and constructs a collection of quads from the result.
     * @param sparqlQuery a SPARQL SELECT query with four variables in the result: named graph, subject, property
     * @param quads collection where the retrieved quads are added
     * @throws InvalidQueryException query error
     * @throws QueryEvaluationException query error
     */
    private void addQuadsFromQuery(String sparqlQuery, Collection<Statement> quads)
            throws InvalidQueryException, QueryEvaluationException {

        TupleQueryResult queryResult = null;
        try {
            queryResult = rdfData.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate();
            while (queryResult.hasNext()) {
                
                BindingSet bindings = queryResult.next();
                Statement quad = VALUE_FACTORY.createStatement(
                        (Resource) bindings.getValue(SUBJECT_VAR),
                        (URI) bindings.getValue(PROPERTY_VAR),
                        bindings.getValue(OBJECT_VAR),
                        rdfData.getDataGraph());
                quads.add(quad);
            }
        } catch (OpenRDFException e) {
            throw new QueryEvaluationException(e);
        } finally {
            if (queryResult != null) {
                queryResult.close();
            }
        }
    }
}
