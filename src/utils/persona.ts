import type { NodePersona } from '@insnh-gd/meristem-shared';

export const PERSONA_AGENT: NodePersona = 'AGENT';
export const PERSONA_GIG: NodePersona = 'GIG';
export const DEFAULT_NODE_PERSONA: NodePersona = PERSONA_AGENT;

export const isNodePersona = (value: unknown): value is NodePersona =>
  value === PERSONA_AGENT || value === PERSONA_GIG;

export const resolveNodePersona = (value: unknown): NodePersona =>
  isNodePersona(value) ? value : DEFAULT_NODE_PERSONA;
