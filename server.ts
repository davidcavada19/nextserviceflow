import express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import next from "next";
import path from "path";
import "dotenv/config";
import { initializeApp } from "firebase/app";
import { getFirestore, doc, getDoc, setDoc } from "firebase/firestore";

// Configuración de Firebase
const firebaseConfig = {
  apiKey: process.env.NEXT_PUBLIC_FIREBASE_API_KEY,
  authDomain: process.env.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN,
  projectId: process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID,
  storageBucket: process.env.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.NEXT_PUBLIC_FIREBASE_APP_ID
};

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);
const SERVICE_DOC_ID = "main_service";

const dev = process.env.NODE_ENV !== "production";
const nextApp = next({ dev });
const handle = nextApp.getRequestHandler();

const PORT = Number(process.env.PORT) || 3000;

async function startServer() {
  try {
    await nextApp.prepare();
    
    const app = express();
    const httpServer = createServer(app);
    
    // Configuración de Socket.io optimizada para producción
    const io = new Server(httpServer, {
      cors: {
        origin: "*", // En producción podrías restringir esto a tu dominio
        methods: ["GET", "POST"]
      },
      transports: ['websocket', 'polling']
    });

    // --- Estado de la Aplicación ---
    let serviceState = {
      id: "1",
      name: "Servicio General",
      status: "scheduled", 
      plannedStartTime: Date.now(),
      actualStartTime: null as number | null,
      blocks: [
        { id: "b1", title: "Bienvenida", responsible: "Pastor", plannedDuration: 300, order: 0, status: "WAITING", actualStartTime: null, actualDuration: null },
        { id: "b2", title: "Cantos", responsible: "Worship", plannedDuration: 420, order: 1, status: "WAITING", actualStartTime: null, actualDuration: null },
        { id: "b3", title: "Sermón", responsible: "Invitado", plannedDuration: 1800, order: 2, status: "WAITING", actualStartTime: null, actualDuration: null },
      ],
    };

    // Cargar estado inicial desde Firebase (si existe)
    let firestoreEnabled = false;
    try {
      const docRef = doc(db, "services", SERVICE_DOC_ID);
      // Timeout manual para el primer getDoc para evitar bloqueos largos si el cliente está "offline" por falta de API
      const docSnap = await getDoc(docRef);
      if (docSnap.exists()) {
        serviceState = { ...serviceState, ...docSnap.data() } as any;
        console.log("> [Firestore] Estado cargado exitosamente.");
        firestoreEnabled = true;
      } else {
        console.log("> [Firestore] No se encontró documento, creando estado inicial...");
        await setDoc(docRef, serviceState);
        firestoreEnabled = true;
      }
    } catch (e: any) {
      console.warn("> [Firestore] Aviso: La API de Firestore parece estar deshabilitada o inaccesible.");
      console.warn("> Detalle:", e.message);
      console.log("> [ServiceFlow] Operando en modo memoria persistente temporal.");
    }

    const saveState = async () => {
      if (!firestoreEnabled) return;
      try {
        await setDoc(doc(db, "services", SERVICE_DOC_ID), serviceState);
      } catch (e) {
        console.error("> [Firestore] Error al guardar cambios:", e);
      }
    };

    // --- Lógica de Sockets ---
    io.on("connection", (socket) => {
      console.log(`Cliente conectado: ${socket.id}`);
      
      socket.emit("service_update", serviceState);

      socket.on("update_planned_start", async (time: number) => {
        serviceState.plannedStartTime = time;
        io.emit("service_update", serviceState);
        await saveState();
      });

      socket.on("add_block", async () => {
        const newBlock = {
          id: Math.random().toString(36).substring(2, 11),
          title: "Nuevo Bloque",
          responsible: "Equipo",
          plannedDuration: 300,
          order: serviceState.blocks.length,
          status: "WAITING",
          actualStartTime: null,
          actualDuration: null
        };
        serviceState.blocks.push(newBlock);
        io.emit("service_update", serviceState);
        await saveState();
      });

      socket.on("delete_block", async (id: string) => {
        serviceState.blocks = serviceState.blocks.filter(b => b.id !== id);
        io.emit("service_update", serviceState);
        await saveState();
      });

      socket.on("start_service", async () => {
        const now = Date.now();
        serviceState.status = "live";
        serviceState.actualStartTime = now;
        
        const firstBlock = serviceState.blocks.find(b => b.order === 0);
        if (firstBlock) {
          firstBlock.status = "LIVE";
          firstBlock.actualStartTime = now;
        }
        
        io.emit("service_update", serviceState);
        await saveState();
      });

      socket.on("update_block", async (blockData) => {
        const index = serviceState.blocks.findIndex(b => b.id === blockData.id);
        if (index !== -1) {
          serviceState.blocks[index] = { ...serviceState.blocks[index], ...blockData };
          io.emit("service_update", serviceState);
          await saveState();
        }
      });

      socket.on("advance_block", async () => {
        const now = Date.now();
        const sortedBlocks = [...serviceState.blocks].sort((a, b) => a.order - b.order);
        const liveIndex = sortedBlocks.findIndex(b => b.status === "LIVE");

        if (liveIndex !== -1) {
          const liveBlock = sortedBlocks[liveIndex];
          const elapsed = liveBlock.actualStartTime
            ? Math.floor((now - liveBlock.actualStartTime) / 1000)
            : liveBlock.plannedDuration;
          const blockIdx = serviceState.blocks.findIndex(b => b.id === liveBlock.id);
          serviceState.blocks[blockIdx] = { ...serviceState.blocks[blockIdx], status: "DONE", actualDuration: elapsed };

          const nextBlock = sortedBlocks[liveIndex + 1];
          if (nextBlock) {
            const nextIdx = serviceState.blocks.findIndex(b => b.id === nextBlock.id);
            serviceState.blocks[nextIdx] = { ...serviceState.blocks[nextIdx], status: "LIVE", actualStartTime: now };
          }
        } else {
          const firstWaiting = sortedBlocks.find(b => b.status === "WAITING");
          if (firstWaiting) {
            const idx = serviceState.blocks.findIndex(b => b.id === firstWaiting.id);
            serviceState.blocks[idx] = { ...serviceState.blocks[idx], status: "LIVE", actualStartTime: now };
          }
        }
        io.emit("service_update", serviceState);
        await saveState();
      });

      socket.on("adjust_block_duration", async (seconds: number) => {
        const liveBlock = serviceState.blocks.find(b => b.status === "LIVE");
        if (liveBlock) {
          const idx = serviceState.blocks.findIndex(b => b.id === liveBlock.id);
          serviceState.blocks[idx] = { ...serviceState.blocks[idx], plannedDuration: Math.max(60, liveBlock.plannedDuration + seconds) };
          io.emit("service_update", serviceState);
          await saveState();
        }
      });

      socket.on("reset_service", async () => {
        serviceState.status = "scheduled";
        serviceState.actualStartTime = null;
        serviceState.blocks = serviceState.blocks.map(b => ({ ...b, status: "WAITING", actualStartTime: null, actualDuration: null }));
        io.emit("service_update", serviceState);
        await saveState();
      });

      socket.on("disconnect", () => {
        console.log(`Cliente desconectado: ${socket.id}`);
      });
    });

    // --- Servir archivos estáticos y Next.js ---
    // En producción, Next.js maneja la optimización
    app.all("*", (req, res) => {
      return handle(req, res);
    });

    httpServer.listen(PORT, () => {
      console.log(`> ServiceFlow listo en http://localhost:${PORT}`);
      console.log(`> Entorno: ${process.env.NODE_ENV || 'development'}`);
    });

  } catch (err) {
    console.error("Error iniciando el servidor:", err);
    process.exit(1);
  }
}

startServer();
