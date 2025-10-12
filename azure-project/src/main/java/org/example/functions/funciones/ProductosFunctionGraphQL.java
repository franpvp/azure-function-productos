package org.example.functions.funciones;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.Scalars;
import graphql.schema.*;

import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class ProductosFunctionGraphQL {
    private static GraphQL graphQL;

    private static Logger safeLogger(Object o) {
        if (o instanceof Logger l) return l;
        return Logger.getLogger("GraphQL");
    }
    private static long toId(Object raw) {
        if (raw == null) throw new IllegalArgumentException("id es requerido");
        if (raw instanceof Number n) return n.longValue();
        if (raw instanceof java.math.BigDecimal bd) return bd.longValue();
        if (raw instanceof java.math.BigInteger bi) return bi.longValue();
        if (raw instanceof char[] ca) return Long.parseLong(new String(ca));
        String s = String.valueOf(raw).trim();
        if (s.isEmpty()) throw new IllegalArgumentException("id vacío");
        return Long.parseLong(s);
    }

    static {
        GraphQLObjectType productoType = GraphQLObjectType.newObject()
                .name("Producto")
                .field(f -> f.name("id").type(Scalars.GraphQLID))
                .field(f -> f.name("nombre").type(Scalars.GraphQLString))
                .field(f -> f.name("descripcion").type(Scalars.GraphQLString))
                .field(f -> f.name("precio").type(Scalars.GraphQLInt))
                .build();

        GraphQLInputObjectType productoInput = GraphQLInputObjectType.newInputObject()
                .name("ProductoInput")
                .field(f -> f.name("nombre").type(new GraphQLNonNull(Scalars.GraphQLString)))
                .field(f -> f.name("descripcion").type(Scalars.GraphQLString))
                .field(f -> f.name("precio").type(Scalars.GraphQLInt))
                .build();

        DataFetcher<Map<String, Object>> productoByIdFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            Object raw = env.getArgument("id");
            long id = toId(raw);
            log.info("productoById id=" + id + " (tipo arg=" + (raw == null ? "null" : raw.getClass().getName()) + ")");
            return obtenerProductoById(id, log);
        };

        DataFetcher<List<Map<String, Object>>> productosFetcher = env ->
                obtenerProductos(safeLogger(env.getGraphQlContext().get("logger")));

        DataFetcher<Map<String, Object>> crearProductoFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            Map<String, Object> input = env.getArgument("input");
            String nombre = trimOrNull(input.get("nombre"));
            String descripcion = trimOrNull(input.get("descripcion"));
            Integer precio = toIntOrNull(input.get("precio"));
            if (nombre == null || nombre.isBlank()) {
                throw new IllegalArgumentException("nombre es requerido");
            }
            long newId = insertarProducto(nombre, descripcion, precio, log);
            log.info("Se ha creado nuevo producto y inserta en tabla para colas");
            return obtenerProductoById(newId, log);
        };

        DataFetcher<Map<String, Object>> actualizarProductoFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            long id = toId(env.getArgument("id"));
            Map<String, Object> input = env.getArgument("input");
            String nombre = trimOrNull(input.get("nombre"));
            String descripcion = trimOrNull(input.get("descripcion"));
            Integer precio = toIntOrNull(input.get("precio"));
            boolean ok = actualizarProducto(id, nombre, descripcion, precio, log);
            if (!ok) return null;
            return obtenerProductoById(id, log);
        };

        DataFetcher<Boolean> eliminarProductoFetcher = env -> {
            Logger log = safeLogger(env.getGraphQlContext().get("logger"));
            Object arg = env.getArgument("id");
            long id = toId(arg);
            log.info("[mutation eliminarProducto] rawArgType=" + (arg == null ? "null" : arg.getClass().getName()) + " -> id=" + id);
            return eliminarProductoById(id, log);
        };

        GraphQLObjectType queryType = GraphQLObjectType.newObject()
                .name("Query")
                .field(f -> f
                        .name("producto")
                        .type(productoType)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .dataFetcher(productoByIdFetcher)
                )
                .field(f -> f
                        .name("productos")
                        .type(GraphQLList.list(productoType))
                        .dataFetcher(productosFetcher)
                )
                .build();

        GraphQLObjectType mutationType = GraphQLObjectType.newObject()
                .name("Mutation")
                .field(f -> f
                        .name("crearProducto")
                        .type(productoType)
                        .argument(GraphQLArgument.newArgument().name("input").type(new GraphQLNonNull(productoInput)))
                        .dataFetcher(crearProductoFetcher)
                )
                .field(f -> f
                        .name("actualizarProducto")
                        .type(productoType)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .argument(GraphQLArgument.newArgument().name("input").type(new GraphQLNonNull(productoInput)))
                        .dataFetcher(actualizarProductoFetcher)
                )
                .field(f -> f
                        .name("eliminarProducto")
                        .type(Scalars.GraphQLBoolean)
                        .argument(GraphQLArgument.newArgument().name("id").type(new GraphQLNonNull(Scalars.GraphQLID)))
                        .dataFetcher(eliminarProductoFetcher)
                )
                .build();

        GraphQLSchema schema = GraphQLSchema.newSchema()
                .query(queryType)
                .mutation(mutationType)
                .build();

        graphQL = GraphQL.newGraphQL(schema).build();
    }

    @FunctionName("graphqlProductos")
    public HttpResponseMessage handleGraphQL(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST, HttpMethod.GET},
                    route = "graphql/productos",
                    authLevel = AuthorizationLevel.ANONYMOUS
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context
    ) {
        Logger log = context.getLogger();

        String query = null;
        Map<String, Object> variables = new HashMap<>();

        try {
            if (request.getHttpMethod() == HttpMethod.POST) {
                String body = request.getBody().orElse("");
                if (body != null && !body.isBlank()) {
                    Map<String, Object> parsed = SimpleJson.parseJsonObject(body);
                    Object q = parsed.get("query");
                    if (q != null) query = String.valueOf(q);
                    Object vars = parsed.get("variables");
                    if (vars instanceof Map) variables = (Map<String, Object>) vars;
                    else if (vars instanceof String && !String.valueOf(vars).isBlank()) {
                        Object obj = SimpleJson.parse(String.valueOf(vars));
                        if (obj instanceof Map) variables = (Map<String, Object>) obj;
                    }
                }
            } else {
                query = request.getQueryParameters().get("query");
                String varsStr = request.getQueryParameters().get("variables");
                if (varsStr != null && !varsStr.isBlank()) {
                    Object obj = SimpleJson.parse(varsStr);
                    if (obj instanceof Map) variables = (Map<String, Object>) obj;
                }
            }
        } catch (Exception e) {
            log.severe("Error parseando request GraphQL: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body(Map.of("errors", List.of(Map.of("message", "Invalid request payload"))))
                    .header("Content-Type", "application/json")
                    .build();
        }

        if (query == null || query.isBlank()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body(Map.of("errors", List.of(Map.of("message", "Ingrese la query"))))
                    .header("Content-Type", "application/json")
                    .build();
        }

        GraphQLContext gqlCtx = GraphQLContext.newContext()
                .of("logger", context.getLogger())
                .build();

        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                .query(query)
                .variables(variables)
                .context(gqlCtx)
                .build();

        ExecutionResult result = graphQL.execute(executionInput);
        Map<String, Object> spec = result.toSpecification();

        return request.createResponseBuilder(HttpStatus.OK)
                .body(spec)
                .header("Content-Type", "application/json")
                .build();
    }

    private static Map<String, Object> obtenerProductoById(long id, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL"); // fallback
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT ID, NOMBRE, DESCRIPCION, PRECIO FROM PRODUCTO WHERE ID=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;

                Map<String, Object> p = new HashMap<>();

                Object idObj = rs.getObject("ID");
                if (idObj instanceof java.math.BigDecimal bd) p.put("id", bd.longValue());
                else if (idObj instanceof Number n)           p.put("id", n.longValue());
                else                                          p.put("id", Long.parseLong(String.valueOf(idObj)));

                Object nombreObj = rs.getObject("NOMBRE");
                p.put("nombre", nombreObj instanceof java.sql.Clob c ? c.getSubString(1, (int) c.length()) : rs.getString("NOMBRE"));

                Object descObj = rs.getObject("DESCRIPCION");
                p.put("descripcion", descObj instanceof java.sql.Clob c ? c.getSubString(1, (int) c.length()) : rs.getString("DESCRIPCION"));

                Object precioObj = rs.getObject("PRECIO");
                if (precioObj != null) {
                    if (precioObj instanceof java.math.BigDecimal bd) p.put("precio", bd.intValue());
                    else if (precioObj instanceof Number n)           p.put("precio", n.intValue());
                    else                                              p.put("precio", Integer.valueOf(String.valueOf(precioObj)));
                }
                return p;
            }
        }
    }

    private static List<Map<String, Object>> obtenerProductos(Logger log) throws Exception {
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT ID, NOMBRE, DESCRIPCION, PRECIO FROM PRODUCTO ORDER BY ID");
             ResultSet rs = ps.executeQuery()) {
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> p = new HashMap<>();
                p.put("id", rs.getLong("ID"));
                p.put("nombre", rs.getString("NOMBRE"));
                p.put("descripcion", rs.getString("DESCRIPCION"));
                int precio = rs.getInt("PRECIO");
                if (!rs.wasNull()) p.put("precio", precio);
                list.add(p);
            }
            return list;
        }
    }

    private static long insertarProducto(String nombre, String descripcion, Integer precio, Logger log) throws Exception {
        String sql = "INSERT INTO PRODUCTO (NOMBRE, DESCRIPCION, PRECIO) VALUES (?, ?, ?)";
        try (Connection conn = open();
             PreparedStatement ps = conn.prepareStatement(sql, new String[]{"ID"})) { // <= mejor que RETURN_GENERATED_KEYS

            ps.setString(1, nombre);
            if (descripcion != null) ps.setString(2, descripcion); else ps.setNull(2, Types.VARCHAR);

            if (precio != null) {
                // si PRECIO es NUMBER(15,2) o NUMBER sin escala, BigDecimal es el mapeo correcto
                ps.setBigDecimal(3, BigDecimal.valueOf(precio));
            } else {
                // DECIMAL/NUMERIC explícito
                ps.setNull(3, Types.DECIMAL);
            }

            ps.executeUpdate();

            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys != null && keys.next()) {
                    return keys.getLong(1);
                }
            }
        }

        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(
                "SELECT ID FROM PRODUCTO WHERE NOMBRE=? AND " +
                        "(DESCRIPCION = ? OR (DESCRIPCION IS NULL AND ? IS NULL)) AND " +
                        "(PRECIO = ? OR (PRECIO IS NULL AND ? IS NULL)) " +
                        "ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY")) {
            ps.setString(1, nombre);
            if (descripcion != null) { ps.setString(2, descripcion); ps.setString(3, descripcion); }
            else { ps.setNull(2, Types.VARCHAR); ps.setNull(3, Types.VARCHAR); }
            if (precio != null) { ps.setDouble(4, precio); ps.setDouble(5, precio); }
            else { ps.setNull(4, Types.NUMERIC); ps.setNull(5, Types.NUMERIC); }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        }

        throw new SQLException("No fue posible obtener el ID generado");
    }

    private static boolean actualizarProducto(long id, String nombre, String descripcion, Integer precio, Logger log) throws Exception {
        List<String> sets = new ArrayList<>();
        if (nombre != null) sets.add("NOMBRE=?");
        if (descripcion != null) sets.add("DESCRIPCION=?");
        if (precio != null) sets.add("PRECIO=?");
        if (sets.isEmpty()) return true;

        String sql = "UPDATE PRODUCTO SET " + String.join(", ", sets) + " WHERE ID=?";
        try (Connection conn = open(); PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            if (nombre != null) ps.setString(idx++, nombre);
            if (descripcion != null) ps.setString(idx++, descripcion);
            if (precio != null) ps.setInt(idx++, precio);
            ps.setLong(idx, id);
            int rows = ps.executeUpdate();
            return rows > 0;
        }
    }

    private static boolean eliminarProductoById(long id, Logger log) throws Exception {
        if (log == null) log = Logger.getLogger("GraphQL");
        try (Connection conn = open()) {
            try (PreparedStatement chk = conn.prepareStatement("SELECT 1 FROM PRODUCTO WHERE ID=?")) {
                chk.setLong(1, id);
                try (ResultSet rs = chk.executeQuery()) {
                    if (!rs.next()) {
                        log.info("[eliminarProductoById] id=" + id + " no existe");
                        return false;
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM PRODUCTO WHERE ID=?")) {
                ps.setLong(1, id);
                int rows = ps.executeUpdate();
                log.info("[eliminarProductoById] id=" + id + " -> rows=" + rows);
                return rows > 0;
            }
        } catch (SQLIntegrityConstraintViolationException fk) {
            log.severe("[eliminarProductoById] FK id=" + id + ": " + fk.getMessage());
            throw new IllegalStateException("No se puede eliminar: producto referenciado (FK).");
        }
    }

    private static Connection open() throws SQLException {
        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        if (walletPath == null || walletPath.isBlank()) {
            walletPath = "/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC";
        }
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";
        return DriverManager.getConnection(url, user, pass);
    }

    private static String trimOrNull(Object o) {
        if (o == null) return null;
        String s = String.valueOf(o).trim();
        return s.isEmpty() ? null : s;
    }
    private static Integer toIntOrNull(Object o) {
        if (o == null) return null;
        try {
            String s = String.valueOf(o).trim();
            if (s.isEmpty()) return null;
            // Permitir valores numéricos enviados como 12.0 para convertirlos a 12
            Double d = Double.valueOf(s);
            return d.intValue();
        } catch (Exception e) {
            return null;
        }
    }

    static class SimpleJson {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @SuppressWarnings("unchecked")
        static Map<String, Object> parseJsonObject(String json) throws Exception {
            if (json == null || json.isBlank()) return new HashMap<>();
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
        }

        static Object parse(String value) throws Exception {
            if (value == null) return null;
            String maybeDecoded = value;
            if (value.contains("%7B") || value.contains("%22") || value.contains("%5B")) {
                maybeDecoded = URLDecoder.decode(value, StandardCharsets.UTF_8);
            }
            try { return MAPPER.readValue(maybeDecoded, new TypeReference<Map<String, Object>>() {}); }
            catch (Exception ignore) { }
            try { return MAPPER.readValue(maybeDecoded, new TypeReference<List<Object>>() {}); }
            catch (Exception ignore) { }
            return maybeDecoded;
        }
    }
}