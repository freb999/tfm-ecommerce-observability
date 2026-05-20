"""
================================================================================
 TFM — Arquitectura de Stream Processing para E-commerce
 Módulo : navigation_graph.py
 Capa   : Análisis Offline y Topología (NetworkX)
 Descripción: 
    Modela matemáticamente el grafo dirigido de estados de navegación.
    Genera un grafo visual contrastando el comportamiento legítimo frente
    a bucles anómalos de bots scrapers.
================================================================================
"""

import networkx as nx
import matplotlib.pyplot as plt
import os

def generate_navigation_graph():
    # 1. Instanciar un Grafo Dirigido (DiGraph)
    G = nx.DiGraph()

    # 2. Definir los Nodos (Estados de la plataforma E-commerce)
    nodos = ["Home", "Catálogo", "Producto", "Carrito", "Checkout", "Compra_Exitosa"]
    G.add_nodes_from(nodos)

    # 3. Definir Aristas (Transiciones/Clicks)
    # Formato: (Origen, Destino, {'weight': intensidad, 'type': tipo_de_trafico})
    aristas = [
        # --- Flujo Humano Normal (Línea Base) ---
        ("Home", "Catálogo", {"weight": 10, "type": "normal"}),
        ("Catálogo", "Producto", {"weight": 8, "type": "normal"}),
        ("Producto", "Carrito", {"weight": 4, "type": "normal"}),
        ("Carrito", "Checkout", {"weight": 2, "type": "normal"}),
        ("Checkout", "Compra_Exitosa", {"weight": 1, "type": "normal"}),
        
        # --- Flujo de Bot / Scraper (Anomalía: Bucle Infinito) ---
        ("Producto", "Catálogo", {"weight": 35, "type": "anomalia"}), # El bot regresa al catálogo
        ("Catálogo", "Producto", {"weight": 35, "type": "anomalia"}), # El bot extrae otro producto
        
        # --- Ataque DoS a pasarela de pago ---
        ("Checkout", "Checkout", {"weight": 50, "type": "ataque_dos"})
    ]
    G.add_edges_from(aristas)

    # 4. Configurar el motor de renderizado visual
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(G, seed=42, k=0.9) # Algoritmo de fuerza para separar nodos

    # Dibujar Nodos
    nx.draw_networkx_nodes(G, pos, node_size=3000, node_color="#1E293B", edgecolors="#0F172A")
    nx.draw_networkx_labels(G, pos, font_size=11, font_color="white", font_weight="bold")

    # Dibujar Aristas por tipo
    aristas_normales = [(u, v) for (u, v, d) in G.edges(data=True) if d["type"] == "normal"]
    aristas_anomalas = [(u, v) for (u, v, d) in G.edges(data=True) if d["type"] in ["anomalia", "ataque_dos"]]

    # Curvar flechas para que se vean los ciclos de ida y vuelta
    nx.draw_networkx_edges(G, pos, edgelist=aristas_normales, width=2, edge_color="#10B981", 
                           arrowsize=20, connectionstyle='arc3, rad=0.1')
    nx.draw_networkx_edges(G, pos, edgelist=aristas_anomalas, width=4, edge_color="#EF4444", 
                           arrowsize=25, style="dashed", connectionstyle='arc3, rad=0.2')

    # 5. Guardar la figura para el documento del TFM
    os.makedirs("data", exist_ok=True)
    output_path = "data/grafo_navegacion_anomalo.png"
    plt.title("Topología de Navegación: Flujo Legítimo (Verde) vs Bucles de Bots/DoS (Rojo)", 
              fontsize=14, fontweight="bold", pad=20)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    print(f"✅ Grafo generado con éxito y guardado en: {output_path}")

if __name__ == "__main__":
    generate_navigation_graph()