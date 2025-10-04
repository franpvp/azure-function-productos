package org.example.functions.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventarioDto {
    private Long id;
    private Long idProducto;
    private Integer cantidadProductos;
    private Long idBodega;
}
