import React from "react";
import Link from "@docusaurus/Link";
import OriginalMDXComponents from "@theme-original/MDXComponents";
import DocusaurusTabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import {
  ArrowDownToLine,
  Bolt,
  BookOpen,
  Brain,
  ChartBar,
  ChartLine,
  ChartScatter,
  Clock,
  Code,
  Database,
  Info as InfoIcon,
  Layers,
  ListOrdered,
  Lock,
  Network,
  Pencil,
  Rocket,
  Search,
  Server,
  Shield,
  Sparkles,
  VectorSquare,
} from "lucide-react";

const CARD_ICONS = {
  "arrow-down-to-line": ArrowDownToLine,
  "book-open": BookOpen,
  "chart-bar": ChartBar,
  "chart-line": ChartLine,
  "chart-scatter": ChartScatter,
  "circle-info": InfoIcon,
  "list-ordered": ListOrdered,
  "magnifying-glass": Search,
  "vector-square": VectorSquare,
  bolt: Bolt,
  brain: Brain,
  clock: Clock,
  code: Code,
  database: Database,
  layers: Layers,
  lock: Lock,
  pencil: Pencil,
  rocket: Rocket,
  server: Server,
  shield: Shield,
  sitemap: Network,
  sparkles: Sparkles,
};

function Field({ kind, path, name, type, required, children, default: defaultValue }) {
  const label = path ?? name;
  return (
    <div className={`membrane-field membrane-field--${kind}`}>
      <div className="membrane-field__header">
        <code>{label}</code>
        {type ? <span className="membrane-field__badge">{type}</span> : null}
        {required ? <span className="membrane-field__required">required</span> : null}
        {defaultValue !== undefined ? (
          <span className="membrane-field__default">default: {String(defaultValue)}</span>
        ) : null}
      </div>
      {children ? <div className="membrane-field__body">{children}</div> : null}
    </div>
  );
}

function ParamField(props) {
  return <Field kind="param" {...props} />;
}

function ResponseField(props) {
  return <Field kind="response" {...props} />;
}

function Expandable({ title = "Details", children }) {
  return (
    <details className="membrane-expandable">
      <summary>{title}</summary>
      <div className="membrane-expandable__body">{children}</div>
    </details>
  );
}

function CodeGroup({ children }) {
  return <div className="membrane-code-group">{children}</div>;
}

function Tabs({ children, ...props }) {
  const tabChildren = React.Children.toArray(children)
    .filter(React.isValidElement)
    .map((child, index) => {
      const {
        title,
        label,
        value,
        children: tabContent,
        ...tabProps
      } = child.props ?? {};
      const tabLabel = title ?? label ?? value ?? `Tab ${index + 1}`;
      const tabValue = value ?? tabLabel;
      return (
        <TabItem key={String(tabValue)} value={String(tabValue)} label={String(tabLabel)} {...tabProps}>
          {tabContent}
        </TabItem>
      );
    });
  return <DocusaurusTabs {...props}>{tabChildren}</DocusaurusTabs>;
}

function Tab({ title, label, value, children, ...props }) {
  const tabLabel = title ?? label ?? value ?? "Tab";
  const tabValue = value ?? tabLabel;
  return (
    <TabItem value={String(tabValue)} label={String(tabLabel)} {...props}>
      {children}
    </TabItem>
  );
}

function Steps({ children }) {
  return <div className="membrane-steps">{children}</div>;
}

function Step({ title, children }) {
  return (
    <div className="membrane-step">
      <div className="membrane-step__content">
        {title ? <h4>{title}</h4> : null}
        {children}
      </div>
    </div>
  );
}

function AdmonitionBox({ kind, title, children }) {
  return (
    <div className={`membrane-admonition membrane-admonition--${kind}`}>
      {title ? <strong>{title}</strong> : null}
      <div>{children}</div>
    </div>
  );
}

function Note({ children, title = "Note" }) {
  return <AdmonitionBox kind="note" title={title}>{children}</AdmonitionBox>;
}

function Info({ children, title = "Info" }) {
  return <AdmonitionBox kind="info" title={title}>{children}</AdmonitionBox>;
}

function Tip({ children, title = "Tip" }) {
  return <AdmonitionBox kind="tip" title={title}>{children}</AdmonitionBox>;
}

function Warning({ children, title = "Warning" }) {
  return <AdmonitionBox kind="warning" title={title}>{children}</AdmonitionBox>;
}

function CardGroup({ children, cols = 2 }) {
  return (
    <div className="membrane-card-group" style={{ "--membrane-card-cols": cols }}>
      {children}
    </div>
  );
}

function CardIcon({ icon }) {
  if (!icon) {
    return null;
  }
  if (React.isValidElement(icon)) {
    return <span className="membrane-card__icon" aria-hidden="true">{icon}</span>;
  }
  const Icon = typeof icon === "string" ? CARD_ICONS[icon] : null;
  if (!Icon) {
    return null;
  }
  return (
    <span className="membrane-card__icon" aria-hidden="true">
      <Icon size={20} strokeWidth={2} />
    </span>
  );
}

function Card({ title, icon, href, children }) {
  const body = (
    <div className="membrane-card">
      <div className="membrane-card__header">
        <CardIcon icon={icon} />
        {title ? <strong>{title}</strong> : null}
      </div>
      <div className="membrane-card__body">{children}</div>
    </div>
  );
  if (!href) {
    return body;
  }
  return (
    <Link className="membrane-card-link" to={href}>
      {body}
    </Link>
  );
}

export default {
  ...OriginalMDXComponents,
  Card,
  CardGroup,
  CodeGroup,
  Expandable,
  Info,
  Note,
  ParamField,
  ResponseField,
  Step,
  Steps,
  Tab,
  Tabs,
  Tip,
  Warning,
};
