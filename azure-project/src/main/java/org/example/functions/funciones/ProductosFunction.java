package org.example.functions.funciones;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.messaging.eventgrid.EventGridEvent;
import com.azure.messaging.eventgrid.EventGridPublisherClient;
import com.azure.messaging.eventgrid.EventGridPublisherClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import org.example.functions.dto.ProductoDto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.util.*;

public class ProductosFunction {

    static final ObjectMapper mapper = new ObjectMapper()
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @FunctionName("obtenerProductos")
    public HttpResponseMessage obtenerProductos(
            @HttpTrigger(
                    name = "reqGetAll",
                    methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS,
                    route = "productos"
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        List<ProductoDto> productos = new ArrayList<>();

        try {
            String walletPath = Optional.ofNullable(System.getenv("ORACLE_WALLET_DIR"))
                    .filter(s -> !s.isBlank())
                    .orElse("/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC");

            String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement stmt = conn.prepareStatement(
                         "SELECT ID, NOMBRE, DESCRIPCION, PRECIO FROM PRODUCTO");
                 ResultSet rs = stmt.executeQuery()) {

                while (rs.next()) {
                    productos.add(ProductoDto.builder()
                            .id(rs.getLong("ID"))
                            .nombre(rs.getString("NOMBRE"))
                            .descripcion(rs.getString("DESCRIPCION"))
                            .precio(rs.getInt("PRECIO"))
                            .build());
                }
            }
        } catch (Exception e) {
            context.getLogger().severe("Error consultando Oracle: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error al obtener productos: " + e.getMessage())
                    .build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(productos)
                .build();
    }

    @FunctionName("getProductoById")
    public HttpResponseMessage getProductoById(
            @HttpTrigger(
                    name = "reqGetById",
                    methods = {HttpMethod.GET},
                    route = "productos/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        ProductoDto producto = null;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("SELECT ID, NOMBRE, DESCRIPCION, PRECIO FROM PRODUCTO WHERE ID = ?")) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                producto = ProductoDto.builder()
                        .id(rs.getLong("ID"))
                        .nombre(rs.getString("NOMBRE"))
                        .descripcion(rs.getString("DESCRIPCION"))
                        .precio(rs.getInt("PRECIO"))
                        .build();
            }
        } catch (Exception e) {
            context.getLogger().severe("Error al obtener producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error: " + e.getMessage())
                    .build();
        }

        if (producto == null) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body("Producto no encontrado con ID " + id)
                    .build();
        }
        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body(producto)
                .build();
    }

    @FunctionName("crearProducto")
    public HttpResponseMessage crearProducto(
            @HttpTrigger(
                    name = "reqPost",
                    methods = {HttpMethod.POST},
                    route = "productos",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        final String eventGridTopicEndpoint = ""; // Configura si publicarás eventos
        final String eventGridTopicKey      = "";

        try {
            String body = request.getBody().orElse("");
            if (body.isEmpty()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body("El body no puede ser vacío")
                        .build();
            }

            ProductoDto nuevo = new ObjectMapper().readValue(body, ProductoDto.class);
            if (nuevo.getNombre() == null || nuevo.getNombre().isBlank()) {
                return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body("El nombre del producto es obligatorio")
                        .build();
            }

            // Conexión Oracle
            String walletPath = Optional.ofNullable(System.getenv("ORACLE_WALLET_DIR"))
                    .filter(s -> !s.isBlank())
                    .orElse("/Users/franciscapalma/Desktop/Bimestre VI/Cloud Native II/Semana 3/azure-project/Wallet_DQXABCOJF1X64NFC");

            String url  = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
            String user = "usuario_test";
            String pass = "Usuariotest2025";

            Long newProductId = null;
            Long inventarioIdUsado = null;
            Long bodegaAsignada    = null;

            try (Connection conn = DriverManager.getConnection(url, user, pass)) {
                conn.setAutoCommit(false);

                // 1) Insertar PRODUCTO
                try (PreparedStatement insProd = conn.prepareStatement(
                        "INSERT INTO PRODUCTO (NOMBRE, DESCRIPCION, PRECIO) VALUES (?, ?, ?)",
                        new String[] {"ID"})) {

                    insProd.setString(1, nuevo.getNombre());
                    insProd.setString(2, nuevo.getDescripcion());
                    insProd.setInt(3, nuevo.getPrecio() == null ? 0 : nuevo.getPrecio()); // null-safe

                    int rows = insProd.executeUpdate();
                    if (rows == 0) {
                        conn.rollback();
                        return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                                .header("Content-Type", "application/json")
                                .body("No se pudo insertar el producto")
                                .build();
                    }
                    try (ResultSet gk = insProd.getGeneratedKeys()) {
                        if (gk != null && gk.next()) newProductId = gk.getLong(1);
                    }
                }

                // Fallback si no es IDENTITY (no ideal, pero útil si aún no migras)
                if (newProductId == null) {
                    try (PreparedStatement sel = conn.prepareStatement(
                            "SELECT MAX(ID) AS ID FROM PRODUCTO WHERE NOMBRE = ?")) {
                        sel.setString(1, nuevo.getNombre());
                        try (ResultSet rs = sel.executeQuery()) {
                            if (rs.next()) newProductId = rs.getLong("ID");
                        }
                    }
                }
                if (newProductId == null) {
                    conn.rollback();
                    return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                            .header("Content-Type", "application/json")
                            .body("No se pudo determinar el ID del producto insertado")
                            .build();
                }

                // 2) Tomar el INVENTARIO de MENOR ID (aunque esté ocupado) y REASIGNARLO
                try (PreparedStatement selMinAny = conn.prepareStatement(
                        "SELECT ID, ID_BODEGA " +
                                "FROM (SELECT ID, ID_BODEGA FROM INVENTARIO ORDER BY ID) " +
                                "WHERE ROWNUM = 1")) {
                    try (ResultSet rs = selMinAny.executeQuery()) {
                        if (rs.next()) {
                            inventarioIdUsado = rs.getLong("ID");
                            bodegaAsignada    = rs.getLong("ID_BODEGA"); // informativo
                        }
                    }
                }

                if (inventarioIdUsado == null) {
                    conn.rollback();
                    return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                            .header("Content-Type", "application/json")
                            .body("{\"error\":\"No existen inventarios en la base de datos para asignar.\"}")
                            .build();
                }

                // 3) Reasignar: setear el nuevo producto y resetear cantidad
                try (PreparedStatement upd = conn.prepareStatement(
                        "UPDATE INVENTARIO SET ID_PRODUCTO = ?, CANTIDAD_PRODUCTOS = 0 WHERE ID = ?")) {
                    upd.setLong(1, newProductId);
                    upd.setLong(2, inventarioIdUsado);
                    upd.executeUpdate();
                }

                conn.commit();

                // 4) Publicar eventos (best-effort)
                try {
                    if (eventGridTopicEndpoint != null && !eventGridTopicEndpoint.isBlank()
                            && eventGridTopicKey != null && !eventGridTopicKey.isBlank()) {
                        EventGridPublisherClient<EventGridEvent> client =
                                new EventGridPublisherClientBuilder()
                                        .endpoint(eventGridTopicEndpoint)
                                        .credential(new AzureKeyCredential(eventGridTopicKey))
                                        .buildEventGridEventPublisherClient();

                        // Evento: producto creado
                        ProductoDto productoEvent = ProductoDto.builder()
                                .id(newProductId)
                                .nombre(nuevo.getNombre())
                                .descripcion(nuevo.getDescripcion())
                                .precio(nuevo.getPrecio())
                                .build();

                        EventGridEvent evProducto = new EventGridEvent(
                                "/api/productos", "api.producto.creado.v1",
                                BinaryData.fromObject(productoEvent), "1.0");
                        evProducto.setEventTime(OffsetDateTime.now());
                        client.sendEvent(evProducto);

                        // Evento: inventario reasignado (existente)
                        Map<String, Object> invEvent = Map.of(
                                "inventarioId", inventarioIdUsado,
                                "idProducto", newProductId,
                                "idBodega", bodegaAsignada,
                                "cantidadProductos", 0,
                                "updatedAt", OffsetDateTime.now().toString()
                        );
                        EventGridEvent evInventario = new EventGridEvent(
                                "/api/inventarios", "api.inventario.reasignado.v1",
                                BinaryData.fromObject(invEvent), "1.0");
                        evInventario.setEventTime(OffsetDateTime.now());
                        client.sendEvent(evInventario);
                    } else {
                        context.getLogger().warning("No se publicaron eventos: faltan EVENTGRID_TOPIC_ENDPOINT/KEY.");
                    }
                } catch (Exception egx) {
                    context.getLogger().severe("Se creó producto y se reasignó inventario, pero falló publicar eventos: " + egx.getMessage());
                }

                // 5) Respuesta
                Map<String, Object> resp = Map.of(
                        "message", "Producto creado y asignado al inventario de menor ID (reasignado)",
                        "producto", Map.of(
                                "id", newProductId,
                                "nombre", nuevo.getNombre(),
                                "descripcion", nuevo.getDescripcion(),
                                "precio", nuevo.getPrecio()
                        ),
                        "inventario", Map.of(
                                "idInventario", inventarioIdUsado,
                                "idBodega", bodegaAsignada,
                                "cantidadProductos", 0
                        )
                );
                return request.createResponseBuilder(HttpStatus.CREATED)
                        .header("Content-Type", "application/json")
                        .body(resp)
                        .build();

            } catch (Exception e) {
                context.getLogger().severe("Error transaccional al crear producto: " + e.getMessage());
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body("{\"error\":\"" + e.getMessage().replace("\"", "\\\"") + "\"}")
                        .build();
            }

        } catch (Exception e) {
            context.getLogger().severe("Error creando producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body("Error al crear producto: " + e.getMessage())
                    .build();
        }
    }

    @FunctionName("modificarProducto")
    public HttpResponseMessage updateProducto(
            @HttpTrigger(
                    name = "reqPut",
                    methods = {HttpMethod.PUT},
                    route = "productos/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<ProductoDto>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        final String eventGridTopicEndpoint = "";
        final String eventGridTopicKey      = "";

        ProductoDto actualizado = request.getBody().orElse(null);
        if (actualizado == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Debe enviar un producto en el body")
                    .build();
        }

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE PRODUCTO SET NOMBRE=?, DESCRIPCION=?, PRECIO=? WHERE ID=?")) {
            stmt.setString(1, actualizado.getNombre());
            stmt.setString(2, actualizado.getDescripcion());
            stmt.setInt(3, actualizado.getPrecio() == null ? 0 : actualizado.getPrecio());
            stmt.setLong(4, id);
            rows = stmt.executeUpdate();
        } catch (Exception e) {
            context.getLogger().severe("Error al actualizar producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }
        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("No existe producto con ID " + id).build();
        }

        try {
            if (!eventGridTopicEndpoint.isBlank() && !eventGridTopicKey.isBlank()) {
                EventGridPublisherClient<EventGridEvent> client = new EventGridPublisherClientBuilder()
                        .endpoint(eventGridTopicEndpoint)
                        .credential(new AzureKeyCredential(eventGridTopicKey))
                        .buildEventGridEventPublisherClient();

                ProductoDto eventData = ProductoDto.builder()
                        .id(id)
                        .nombre(actualizado.getNombre())
                        .descripcion(actualizado.getDescripcion())
                        .precio(actualizado.getPrecio())
                        .build();

                EventGridEvent event = new EventGridEvent(
                        "/api/productos",
                        "api.producto.actualizado.v1",
                        BinaryData.fromObject(eventData),
                        "1.0"
                );
                event.setEventTime(OffsetDateTime.now());
                client.sendEvent(event);
            }
        } catch (Exception e) {
            context.getLogger().severe("Producto actualizado pero falló publicar evento: " + e.getMessage());
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("Producto actualizado con éxito")
                .build();
    }

    @FunctionName("eliminarProducto")
    public HttpResponseMessage deleteProducto(
            @HttpTrigger(
                    name = "reqDelete",
                    methods = {HttpMethod.DELETE},
                    route = "productos/{id}",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) {

        String walletPath = System.getenv("ORACLE_WALLET_DIR");
        String url = "jdbc:oracle:thin:@dqxabcojf1x64nfc_tp?TNS_ADMIN=" + walletPath;
        String user = "usuario_test";
        String pass = "Usuariotest2025";

        int rows;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM PRODUCTO WHERE ID=?")) {
            stmt.setLong(1, id);
            rows = stmt.executeUpdate();
        } catch (Exception e) {
            context.getLogger().severe("Error al eliminar producto: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage()).build();
        }

        if (rows == 0) {
            return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                    .body("No existe producto con ID " + id).build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .body("Producto eliminado con éxito").build();
    }

    // Utilidad JSON
    private HttpResponseMessage json(HttpRequestMessage<?> req, HttpStatus status, Object body) {
        return req.createResponseBuilder(status)
                .header("Content-Type", "application/json")
                .body(asJson(body))
                .build();
    }

    private String asJson(Object o) {
        try { return mapper.writeValueAsString(o); }
        catch (Exception e) { return "{\"error\":\"No se pudo serializar JSON\"}"; }
    }
}