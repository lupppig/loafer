'use client'

import { useEffect, useLayoutEffect, useMemo, useRef } from 'react'
import type { ReactNode, RefObject } from 'react'
import { Canvas, useFrame, useThree } from '@react-three/fiber'
import * as THREE from 'three'

/*
 * The routing yard.
 *
 * skills/loafer-web-ui/references/visual-direction.md asks for one coherent
 * world: sources are physical docks, bounded batches are visible parcels,
 * validation gates inspect them, transforms reshape them, and targets receive
 * sealed, counted output. This scene is that sentence, built to scale.
 *
 * It is deliberately literal about one thing the copy also insists on: parcels
 * arrive in batches of four with a gap between them, not as an infinite
 * decorative stream, because bounded batching is the actual product claim. A
 * parcel is steel-grey until it clears the transform press, and orange after —
 * the run has committed to it by then.
 *
 * Budget: four small block meshes, one instanced mesh for every parcel, two
 * lights, no shadow maps, no environment map, no post-processing. The whole
 * scene is a few hundred triangles.
 */

const INK_RAISED = '#1a1e1d'
const INK_SURFACE = '#131615'
const STEEL = '#333a38'
const STEEL_STRONG = '#646d69'
const SIGNAL = '#d95f2a'
const PARCEL_RAW = '#5b6562'

const STATION = {
  source: -5.15,
  validate: -1.75,
  transform: 1.75,
  target: 5.15,
} as const

/*
 * The horizontal span the yard occupies in world units, including the run-up
 * before the dock. `Parallax` scales by this so the whole run stays inside the
 * frame at every viewport width instead of the target bin falling off the
 * right edge.
 */
const YARD_SPAN = 14.4

/** Where parcels enter and where they stop travelling horizontally. */
const TRAVEL_START = -7.7
const TRAVEL_END = STATION.target
/** Fraction of the cycle spent moving; the remainder is the drop into the bin. */
const TRAVEL_PHASE = 0.86
const CYCLE_SECONDS = 9.5

const PARCEL_COUNT = 16
const BATCH_SIZE = 4
const PARCEL_SIZE = 0.44

/* Four batches evenly spaced around the cycle, each batch tight. */
const PARCEL_OFFSETS = Array.from({ length: PARCEL_COUNT }, (_, i) => {
  const batch = Math.floor(i / BATCH_SIZE)
  const withinBatch = i % BATCH_SIZE
  return (batch / (PARCEL_COUNT / BATCH_SIZE) + withinBatch * 0.042) % 1
})

/**
 * A box with its edges drawn, which is what makes the scene read as stamped
 * plate rather than as untextured grey geometry.
 */
function Block({
  size,
  position,
  color = INK_RAISED,
  edge = STEEL,
  opacity = 1,
}: {
  size: [number, number, number]
  position: [number, number, number]
  color?: string
  edge?: string
  opacity?: number
}) {
  // Destructured so the geometry is rebuilt on an actual dimension change,
  // not on every render because the caller passed a fresh array literal.
  const [width, height, depth] = size
  const geometry = useMemo(() => new THREE.BoxGeometry(width, height, depth), [width, height, depth])
  const edges = useMemo(() => new THREE.EdgesGeometry(geometry), [geometry])

  useEffect(() => {
    return () => {
      geometry.dispose()
      edges.dispose()
    }
  }, [geometry, edges])

  return (
    <group position={position}>
      <mesh geometry={geometry}>
        <meshStandardMaterial
          color={color}
          roughness={0.72}
          metalness={0.08}
          transparent={opacity < 1}
          opacity={opacity}
        />
      </mesh>
      <lineSegments geometry={edges}>
        <lineBasicMaterial color={edge} transparent opacity={0.85} />
      </lineSegments>
    </group>
  )
}

const SLEEPER_COUNT = 15

/**
 * The conveyor the batches ride, plus its sleepers. The sleepers are one
 * instanced draw call rather than seventeen meshes; they are pure texture and
 * do not deserve their own geometry each.
 */
function Rail() {
  const sleepers = useRef<THREE.InstancedMesh>(null)

  useLayoutEffect(() => {
    const instanced = sleepers.current
    if (!instanced) return
    const dummy = new THREE.Object3D()
    for (let i = 0; i < SLEEPER_COUNT; i += 1) {
      dummy.position.set(-7.6 + i * 1.02, -0.42, 0)
      dummy.updateMatrix()
      instanced.setMatrixAt(i, dummy.matrix)
    }
    instanced.instanceMatrix.needsUpdate = true
  }, [])

  return (
    <group>
      <Block size={[15.2, 0.16, 1.5]} position={[-0.3, -0.32, 0]} color={INK_SURFACE} />
      <instancedMesh
        ref={sleepers}
        args={[undefined as never, undefined as never, SLEEPER_COUNT]}
        frustumCulled={false}
      >
        <boxGeometry args={[0.09, 0.1, 1.9]} />
        <meshStandardMaterial color={STEEL} roughness={0.8} />
      </instancedMesh>
    </group>
  )
}

/** The source dock: a stack of loaded crates feeding the rail. */
function SourceDock() {
  return (
    <group position={[STATION.source, 0, 0]}>
      <Block size={[1.7, 1.5, 1.7]} position={[0, 0.5, 0]} />
      <Block size={[1.15, 0.1, 1.15]} position={[0, 1.32, 0]} color={STEEL} />
      {[0.15, 0.52, 0.89].map((y) => (
        <Block key={y} size={[1.78, 0.05, 1.78]} position={[0, y, 0]} color={STEEL_STRONG} />
      ))}
    </group>
  )
}

/**
 * The validation gate. The ring brightens as a parcel passes through it, which
 * is the only motion in the scene that is a reaction rather than a loop.
 */
function ValidateGate({ activityRef }: { activityRef: RefObject<number> }) {
  const ring = useRef<THREE.MeshStandardMaterial>(null)

  useFrame(() => {
    if (!ring.current) return
    ring.current.emissiveIntensity = 0.15 + activityRef.current * 1.5
  })

  return (
    <group position={[STATION.validate, 0, 0]}>
      <Block size={[0.22, 2.3, 0.22]} position={[0, 0.75, 0.9]} />
      <Block size={[0.22, 2.3, 0.22]} position={[0, 0.75, -0.9]} />
      <Block size={[0.22, 0.22, 2]} position={[0, 1.8, 0]} />
      {/* The inspection beam across the opening. */}
      <mesh position={[0, 0.55, 0]} rotation={[Math.PI / 2, 0, 0]}>
        <torusGeometry args={[0.62, 0.035, 8, 32]} />
        <meshStandardMaterial
          ref={ring}
          color={SIGNAL}
          emissive={SIGNAL}
          emissiveIntensity={0.15}
          roughness={0.4}
        />
      </mesh>
    </group>
  )
}

/** The transform press: two plates that close on a parcel as it passes. */
function TransformPress({ activityRef }: { activityRef: RefObject<number> }) {
  const upper = useRef<THREE.Group>(null)
  const lower = useRef<THREE.Group>(null)

  useFrame(() => {
    const close = activityRef.current
    if (upper.current) upper.current.position.y = 1.55 - close * 0.52
    if (lower.current) lower.current.position.y = -0.5 + close * 0.16
  })

  return (
    <group position={[STATION.transform, 0, 0]}>
      <Block size={[0.18, 2.6, 0.18]} position={[0, 0.9, 1.05]} color={INK_SURFACE} />
      <Block size={[0.18, 2.6, 0.18]} position={[0, 0.9, -1.05]} color={INK_SURFACE} />
      <group ref={upper} position={[0, 1.55, 0]}>
        <Block size={[1.5, 0.32, 2.3]} position={[0, 0, 0]} />
      </group>
      <group ref={lower} position={[0, -0.5, 0]}>
        <Block size={[1.5, 0.22, 2.3]} position={[0, 0, 0]} />
      </group>
    </group>
  )
}

/** The target: an open bin with sealed output already settled in it. */
function TargetBin() {
  return (
    <group position={[STATION.target, 0, 0]}>
      <Block size={[2.3, 0.16, 2.3]} position={[0, -0.72, 0]} />
      <Block size={[0.14, 1.3, 2.3]} position={[1.08, -0.15, 0]} />
      <Block size={[0.14, 1.3, 2.3]} position={[-1.08, -0.15, 0]} />
      <Block size={[2.3, 1.3, 0.14]} position={[0, -0.15, -1.08]} />
      {/* Settled parcels. Sealed, so they carry the signal colour. */}
      {[
        [-0.5, -0.42, -0.45],
        [0.05, -0.42, 0.1],
        [0.55, -0.42, -0.35],
        [-0.2, -0.42, 0.55],
        [-0.25, 0.02, -0.1],
        [0.35, 0.02, 0.4],
      ].map(([x, y, z], i) => (
        <Block
          key={i}
          size={[PARCEL_SIZE, PARCEL_SIZE, PARCEL_SIZE]}
          position={[x, y, z]}
          color={SIGNAL}
          edge={'#f0a583'}
        />
      ))}
    </group>
  )
}

/**
 * Every in-flight batch, as one instanced draw call.
 *
 * Also the scene's clock: it computes how close the nearest parcel is to the
 * gate and to the press, and hands those two numbers to the stations so their
 * reactions stay in sync with the geometry rather than running on their own
 * timers.
 */
function Parcels({
  gateRef,
  pressRef,
  runningRef,
}: {
  gateRef: RefObject<number>
  pressRef: RefObject<number>
  runningRef: RefObject<boolean>
}) {
  const mesh = useRef<THREE.InstancedMesh>(null)
  const elapsed = useRef(0)

  /*
   * Scratch objects, allocated once on the first frame and reused after.
   * They live in a ref rather than a `useMemo` because they exist precisely to
   * be mutated sixty times a second, and a memo result is not a legal place to
   * do that.
   */
  const scratchRef = useRef<{
    dummy: THREE.Object3D
    raw: THREE.Color
    sealed: THREE.Color
    color: THREE.Color
  } | null>(null)

  useFrame((_, delta) => {
    const instanced = mesh.current
    if (!instanced) return

    const scratchpad = (scratchRef.current ??= {
      dummy: new THREE.Object3D(),
      raw: new THREE.Color(PARCEL_RAW),
      sealed: new THREE.Color(SIGNAL),
      color: new THREE.Color(),
    })
    const { dummy, raw: rawColor, sealed: sealedColor, color: scratch } = scratchpad

    // Advance our own clock so pausing genuinely stops time rather than
    // letting the scene jump forward when it resumes.
    if (runningRef.current) elapsed.current += Math.min(delta, 0.05)
    const t = elapsed.current / CYCLE_SECONDS

    let nearestGate = Infinity
    let nearestPress = Infinity

    for (let i = 0; i < PARCEL_COUNT; i += 1) {
      const phase = (t + PARCEL_OFFSETS[i]) % 1

      let x: number
      let y = PARCEL_SIZE / 2 - 0.1
      let scale = 1

      if (phase < TRAVEL_PHASE) {
        x = TRAVEL_START + (phase / TRAVEL_PHASE) * (TRAVEL_END - TRAVEL_START)
      } else {
        // Past the rail: drop into the bin and settle out of sight.
        const drop = (phase - TRAVEL_PHASE) / (1 - TRAVEL_PHASE)
        x = TRAVEL_END
        y = PARCEL_SIZE / 2 - 0.1 - drop * 0.62
        scale = 1 - Math.max(0, drop - 0.55) / 0.45
      }

      // A parcel lifts very slightly as the press releases it, so the press
      // reads as acting on the parcel rather than near it.
      const pressProximity = Math.abs(x - STATION.transform)
      if (pressProximity < 0.9) y += (0.9 - pressProximity) * 0.08

      nearestGate = Math.min(nearestGate, Math.abs(x - STATION.validate))
      nearestPress = Math.min(nearestPress, pressProximity)

      dummy.position.set(x, y, 0)
      dummy.rotation.set(0, 0, 0)
      dummy.scale.setScalar(Math.max(0, scale))
      dummy.updateMatrix()
      instanced.setMatrixAt(i, dummy.matrix)

      // Colour is the transform boundary made visible: raw before the press,
      // sealed after, with a short blend across it rather than a hard switch.
      const sealed = THREE.MathUtils.smoothstep(x, STATION.transform - 0.5, STATION.transform + 0.5)
      scratch.copy(rawColor).lerp(sealedColor, sealed)
      instanced.setColorAt(i, scratch)
    }

    instanced.instanceMatrix.needsUpdate = true
    if (instanced.instanceColor) instanced.instanceColor.needsUpdate = true

    gateRef.current = Math.max(0, 1 - nearestGate / 0.85)
    pressRef.current = Math.max(0, 1 - nearestPress / 0.75)
  })

  return (
    <instancedMesh
      ref={mesh}
      args={[undefined as never, undefined as never, PARCEL_COUNT]}
      frustumCulled={false}
    >
      <boxGeometry args={[PARCEL_SIZE, PARCEL_SIZE, PARCEL_SIZE]} />
      <meshStandardMaterial roughness={0.6} metalness={0.12} />
    </instancedMesh>
  )
}

/**
 * A very shallow pointer parallax. The yard tilts toward the cursor by a few
 * degrees, which is enough to establish that it is a solid object in space and
 * not a flat illustration, and small enough that it never fights the copy for
 * attention.
 */
function Parallax({
  children,
  enabled,
}: {
  children: ReactNode
  enabled: boolean
}) {
  const group = useRef<THREE.Group>(null)
  const { viewport } = useThree()

  useFrame((state) => {
    if (!group.current) return
    const targetY = enabled ? state.pointer.x * 0.12 : 0
    const targetX = enabled ? -state.pointer.y * 0.05 : 0
    group.current.rotation.y += (targetY - group.current.rotation.y) * 0.045
    group.current.rotation.x += (targetX - group.current.rotation.x) * 0.045
  })

  // Fit the yard to the frame. Rotating it off-axis foreshortens the run, so
  // the effective span is the rotated width; the 0.94 accounts for that and
  // the small margin keeps the registration marks clear of the geometry.
  // The lower bound matters: `viewport` reads 0 on the very first frame, and
  // without a floor the yard would be scaled to nothing and never recover if
  // the frame loop were paused before a resize ever arrived.
  const scale = Math.min(1.15, Math.max(0.55, (viewport.width * 1.02) / YARD_SPAN))

  return (
    <group ref={group} scale={scale}>
      {children}
    </group>
  )
}

function Yard({ animate, runningRef }: { animate: boolean; runningRef: RefObject<boolean> }) {
  const gate = useRef(0)
  const press = useRef(0)

  return (
    <Parallax enabled={animate}>
      <group position={[-0.2, -0.2, 0]} rotation={[0, -0.52, 0]}>
        <Rail />
        <SourceDock />
        <ValidateGate activityRef={gate} />
        <TransformPress activityRef={press} />
        <TargetBin />
        <Parcels gateRef={gate} pressRef={press} runningRef={runningRef} />
      </group>
    </Parallax>
  )
}

export interface FoundrySceneProps {
  /** False under `prefers-reduced-motion`: the yard renders one static frame. */
  animate: boolean
  /** False when scrolled out of view or the tab is hidden. */
  active: boolean
}

export default function FoundryScene({ animate, active }: FoundrySceneProps) {
  // Held in a ref rather than passed down as a prop so that pausing and
  // resuming never re-renders the scene graph — only the frame loop reads it.
  const running = useRef(animate && active)
  useEffect(() => {
    running.current = animate && active
  }, [animate, active])

  return (
    <Canvas
      // A single static frame is all a reduced-motion or paused scene needs, so
      // the render loop is only allowed to run when something is moving.
      frameloop={animate && active ? 'always' : 'demand'}
      dpr={[1, 1.75]}
      gl={{ antialias: true, powerPreference: 'low-power', alpha: true }}
      camera={{ position: [0, 4.15, 12.3], fov: 32 }}
      onCreated={({ camera }) => camera.lookAt(0, 0.3, 0)}
      style={{ touchAction: 'pan-y' }}
    >
      <ambientLight intensity={1.7} />
      <directionalLight position={[-6, 9, 7]} intensity={3.2} color="#f4efe6" />
      <directionalLight position={[7, 3, -5]} intensity={1.15} color="#8fa0aa" />
      {/* Warm bounce from the press, so the signal colour has a source. */}
      <pointLight position={[STATION.transform, 1.4, 1.6]} intensity={6} distance={7} color={SIGNAL} />
      <Yard animate={animate} runningRef={running} />
    </Canvas>
  )
}
