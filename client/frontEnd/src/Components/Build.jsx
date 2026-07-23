import { useCallback, useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import "../Styles/Build.css";
import pc from "../assets/custom-gaming-pc.png";
import performanceLabHardware from "../assets/performance-lab-hardware.webp";
import processorImg from "../assets/Processor-Background-PNG-Image.webp";
import motherboardImg from "../assets/Motherboard-PNG.webp";
import graphicCardImg from "../assets/graphics-card-image.webp";
import storageImg from "../assets/pngwing.com.webp";
import RAMImg from "../assets/RAM-Memory-Transparent.webp";
import SmpsImg from "../assets/SMPS-image.webp";
import api from "../services/api";
import BuildSummary from "./builder/BuildSummary";
import BuildStatusDock from "./builder/BuildStatusDock";
import CompatibilityRail from "./builder/CompatibilityRail";
import ComponentPicker from "./builder/ComponentPicker";
import HardwareStage from "./builder/HardwareStage";
import { useSession } from "../auth/SessionContext";
import {
  calculateRecommendedPsu,
  canVisitStep,
  emptySelection,
  estimatePerformance,
  getItemsForStep,
  getFirstIncompleteStep,
  getRailSteps,
  getRuleEvidence,
  getSelectedForStep,
  hydrateSavedBuild,
  isBuildComplete,
  isStepSelected,
  resetAfterStep,
  stepLabels,
  stepOrder,
} from "./builder/buildUtils";
import { clearBuildDraft, loadBuildDraft, saveBuildDraft } from "./builder/buildDraft";

const imagePaths = {
  platform: processorImg,
  processor: processorImg,
  motherboard: motherboardImg,
  gpu: graphicCardImg,
  primaryStorage: storageImg,
  secondaryStorage: storageImg,
  ram: RAMImg,
  smps: SmpsImg,
  cabinet: pc,
  review: pc,
};

const stepDescriptions = {
  platform: "Choose the CPU family. This constrains the processor list.",
  processor: "Select a CPU. Motherboards will be filtered by its socket.",
  motherboard: "Pick a board that matches the CPU socket and exposes the right form factor.",
  gpu: "Choose the graphics card used for power and performance estimates.",
  primaryStorage: "Select an NVMe drive for the primary storage slot.",
  secondaryStorage: "Select a SATA drive for secondary storage.",
  ram: "Pick memory that matches the motherboard-supported RAM generation.",
  smps: "Choose a power supply at or above the calculated wattage target.",
  cabinet: "Choose a cabinet that supports the selected motherboard form factor.",
};

const emptyCatalog = {
  gpu: [],
  cpu: [],
  cabinet: [],
  storage: [],
  smps: [],
  motherboard: [],
  ram: [],
};

const Build = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const [initialDraft] = useState(() =>
    location.state?.newBuild ? null : loadBuildDraft()
  );
  const [catalog, setCatalog] = useState(emptyCatalog);
  const [catalogStatus, setCatalogStatus] = useState("idle");
  const [catalogError, setCatalogError] = useState("");
  const [selection, setSelection] = useState(initialDraft?.selection || emptySelection);
  const [currentStepId, setCurrentStepId] = useState(initialDraft?.currentStepId || "platform");
  const [sourceSaveId, setSourceSaveId] = useState(initialDraft?.sourceSaveId || null);
  const [saveState, setSaveState] = useState("idle");
  const [message, setMessage] = useState("");
  const [serverCompatibility, setServerCompatibility] = useState(null);
  const [compatibilityStatus, setCompatibilityStatus] = useState("idle");
  const [serverAnalytics, setServerAnalytics] = useState(null);
  const { isAuthenticated } = useSession();

  const loadCatalog = useCallback(async () => {
    setCatalogStatus("loading");
    setCatalogError("");

    try {
      const response = await api.get("/api/v1/catalog");
      const data = response.data.data;

      setCatalog({
        gpu: data.gpus,
        cpu: data.processors,
        cabinet: data.cabinets,
        storage: data.storage,
        smps: data.powerSupplies,
        motherboard: data.motherboards,
        ram: data.ram,
      });
      setCatalogStatus("ready");
    } catch (error) {
      console.error("Error fetching component catalog:", error);
      setCatalogError("Unable to load component catalog.");
      setCatalogStatus("error");
    }
  }, []);

  useEffect(() => {
    loadCatalog();
  }, [loadCatalog]);

  useEffect(() => {
    saveBuildDraft({ selection, currentStepId, sourceSaveId });
  }, [currentStepId, selection, sourceSaveId]);

  useEffect(() => {
    const componentIds = {
      processor: selection.processor?._id,
      motherboard: selection.motherboard?._id,
      gpu: selection.gpu?._id,
      primaryStorage: selection.primaryStorage?._id,
      secondaryStorage: selection.secondaryStorage?._id,
      ram: selection.ram?._id,
      smps: selection.smps?._id,
      cabinet: selection.cabinet?._id,
    };

    if (!Object.values(componentIds).some(Boolean)) {
      setServerCompatibility(null);
      setCompatibilityStatus("idle");
      return;
    }

    let active = true;
    setCompatibilityStatus("checking");
    api.post("/api/v1/compatibility/evaluate", { componentIds })
      .then((response) => {
        if (!active) return;
        setServerCompatibility(response.data);
        setCompatibilityStatus("ready");
      })
      .catch((error) => {
        if (!active) return;
        console.error("Compatibility evaluation failed:", error);
        setServerCompatibility(null);
        setCompatibilityStatus("error");
      });

    return () => { active = false; };
  }, [selection]);

  useEffect(() => {
    if (!selection.processor?._id || !selection.gpu?._id) {
      setServerAnalytics(null);
      return;
    }

    let active = true;
    api.post("/api/v1/analytics/estimate", {
      componentIds: { processor: selection.processor._id, gpu: selection.gpu._id },
    })
      .then((response) => {
        if (active) setServerAnalytics(response.data);
      })
      .catch((error) => {
        if (!active) return;
        console.error("Performance estimate failed:", error);
        setServerAnalytics(null);
      });

    return () => { active = false; };
  }, [selection.gpu, selection.processor]);

  useEffect(() => {
    if (location.state?.newBuild) {
      clearBuildDraft();
      setSelection(emptySelection);
      setCurrentStepId("platform");
      setSourceSaveId(null);
      setMessage("");
      navigate(location.pathname, { replace: true, state: null });
      return;
    }

    if (location.state?.savedBuild && catalogStatus === "ready") {
      const hydrated = hydrateSavedBuild(location.state.savedBuild, catalog);
      const nextStep = getFirstIncompleteStep(hydrated);
      setSelection(hydrated);
      setCurrentStepId(nextStep);
      setSourceSaveId(location.state.savedBuild._id);
      setMessage(nextStep === "review" ? "Saved build loaded. Changes will update this record." : "Some saved parts are no longer in the catalog. Review the highlighted step.");
      navigate(location.pathname, { replace: true, state: null });
    }
  }, [catalog, catalogStatus, location.pathname, location.state, navigate]);

  const estimate = useMemo(
    () => ({
      psuTarget: calculateRecommendedPsu(selection.processor, selection.gpu),
      performance: serverAnalytics?.performance
        ? {
            ...serverAnalytics.performance,
            confidence: serverAnalytics.confidence,
            modelVersion: serverAnalytics.model?.version,
          }
        : estimatePerformance(selection.processor, selection.gpu),
    }),
    [selection.gpu, selection.processor, serverAnalytics]
  );

  const railSteps = useMemo(
    () => getRailSteps(selection, currentStepId),
    [currentStepId, selection]
  );

  const currentItems = useMemo(
    () => getItemsForStep(currentStepId, selection, catalog),
    [catalog, currentStepId, selection]
  );

  const selectedPart = getSelectedForStep(selection, currentStepId);
  const selectedId = currentStepId === "platform" ? selection.platform : selectedPart?._id;
  const currentImage = imagePaths[currentStepId] || pc;

  const setSelectedForStep = (stepId, item) => {
    setMessage("");
    setSaveState("idle");
    setSelection((previousSelection) => {
      const nextSelection = resetAfterStep(previousSelection, stepId);
      if (stepId === "platform") {
        nextSelection.platform = item._id;
      } else {
        nextSelection[stepId] = item;
        if (stepId === "secondaryStorage") nextSelection.skipSecondaryStorage = false;
      }
      return nextSelection;
    });
  };

  const handleSkipSecondaryStorage = () => {
    setSelection((previousSelection) => ({
      ...resetAfterStep(previousSelection, "secondaryStorage"),
      secondaryStorage: null,
      skipSecondaryStorage: true,
    }));
    setCurrentStepId("ram");
    setMessage("");
  };

  const handleRailStepSelect = (stepId) => {
    if (!canVisitStep(selection, stepId)) {
      return;
    }
    setCurrentStepId(stepId);
    setMessage("");
  };

  const handleContinue = () => {
    if (!isStepSelected(selection, currentStepId)) {
      setMessage("Select a compatible option before continuing.");
      return;
    }

    const currentIndex = stepOrder.indexOf(currentStepId);
    const nextStep = stepOrder[currentIndex + 1] || "review";
    setCurrentStepId(nextStep);
    setMessage("");
  };

  const handleSave = async () => {
    if (!isBuildComplete(selection)) {
      setMessage("Complete every compatibility step before saving.");
      return;
    }

    if (!isAuthenticated) {
      setMessage("Sign in to save this build.");
      return;
    }

    setSaveState("saving");
    setMessage("");

    try {
      const payload = {
        cpu: selection.processor.name,
        motherboard: selection.motherboard.name,
        gpu: selection.gpu.name,
        primaryStorage: selection.primaryStorage.name,
        secondaryStorage: selection.secondaryStorage?.name || "",
        ram: selection.ram.name,
        powerSupply: selection.smps.name,
        cabinet: selection.cabinet.name,
        image: selection.cabinet.image_url || pc,
        componentIds: {
          processor: selection.processor._id,
          motherboard: selection.motherboard._id,
          gpu: selection.gpu._id,
          primaryStorage: selection.primaryStorage._id,
          secondaryStorage: selection.secondaryStorage?._id,
          ram: selection.ram._id,
          smps: selection.smps._id,
          cabinet: selection.cabinet._id,
        },
      };
      const response = sourceSaveId
        ? await api.put(`/saves/${sourceSaveId}`, payload)
        : await api.post("/saves", payload);

      if (response.status === 200 || response.status === 201) {
        clearBuildDraft();
        navigate("/profile");
      } else {
        setSaveState("error");
        setMessage("Save failed. Try again.");
      }
    } catch (error) {
      console.error("Error during save:", error);
      setSaveState("error");
      setMessage(error.response?.data?.error || "Save failed. Try again.");
    }
  };

  const isCatalogLoading = catalogStatus === "loading" && currentStepId !== "platform";
  const pickerDescription =
    catalogError && currentStepId !== "platform"
      ? getRuleEvidence(selection, currentStepId)
      : stepDescriptions[currentStepId];

  return (
    <div className="Build_Page">
      <div className="Build-Content">
        <CompatibilityRail
          steps={railSteps}
          currentStepId={currentStepId}
          onStepSelect={handleRailStepSelect}
        />

        <HardwareStage
          stepId={currentStepId}
          title={stepLabels[currentStepId]}
          selection={selection}
          selectedPart={selectedPart}
          image={currentStepId === "platform" ? performanceLabHardware : currentImage}
        />

        <div className="Build-Area">
          {currentStepId === "review" ? (
            <BuildSummary
              selection={selection}
              estimate={estimate}
              saveState={saveState}
              message={message}
              sourceSaveId={sourceSaveId}
              compatibility={serverCompatibility}
              compatibilityStatus={compatibilityStatus}
              onSave={handleSave}
              onBack={() => setCurrentStepId("cabinet")}
            />
          ) : (
            <ComponentPicker
              stepId={currentStepId}
              title={stepLabels[currentStepId]}
              description={pickerDescription}
              items={currentItems}
              selectedId={selectedId}
              loading={isCatalogLoading}
              error={currentStepId === "platform" ? "" : catalogError}
              onRetry={loadCatalog}
              onSelect={(item) => setSelectedForStep(currentStepId, item)}
              onSkip={handleSkipSecondaryStorage}
              canContinue={isStepSelected(selection, currentStepId)}
              rowImage={currentImage}
            />
          )}
        </div>
      </div>

      <BuildStatusDock
        selection={selection}
        estimate={estimate}
        compatibility={serverCompatibility}
        compatibilityStatus={compatibilityStatus}
        canContinue={isStepSelected(selection, currentStepId)}
        isReview={currentStepId === "review"}
        onContinue={handleContinue}
      />
    </div>
  );
};

export default Build;
