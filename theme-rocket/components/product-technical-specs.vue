<template>
  <section class="yampi-tech-specs" v-if="hasAnyContent">
    <div class="yampi-tech-specs__tabs" role="tablist">
      <button
        v-for="tab in availableTabs"
        :key="tab.id"
        type="button"
        class="yampi-tech-specs__tab"
        :class="{ 'is-active': activeTab === tab.id }"
        @click="activeTab = tab.id"
      >
        {{ tab.label }}
      </button>
    </div>

    <div class="yampi-tech-specs__panel" v-show="activeTab === 'description'">
      <div class="yampi-tech-specs__html" v-html="description"></div>
    </div>

    <div class="yampi-tech-specs__panel" v-show="activeTab === 'specs'">
      <table class="yampi-tech-specs__table">
        <tbody>
          <tr v-for="(item, index) in parsedSpecifications" :key="index">
            <th>{{ item.label }}</th>
            <td>{{ item.value }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="yampi-tech-specs__panel" v-show="activeTab === 'measures'">
      <p class="yampi-tech-specs__measures">{{ measures }}</p>
    </div>
  </section>
</template>

<script>
export default {
  name: 'ProductTechnicalSpecs',
  props: {
    description: { type: String, default: '' },
    specifications: { type: String, default: '' },
    measures: { type: String, default: '' }
  },
  data() {
    return {
      activeTab: 'description'
    };
  },
  computed: {
    parsedSpecifications() {
      if (!this.specifications) return [];
      return this.specifications
        .split(';')
        .map(part => part.trim())
        .filter(Boolean)
        .map(part => {
          const [label, ...rest] = part.split(':');
          return {
            label: (label || 'Característica').trim(),
            value: rest.join(':').trim() || '-'
          };
        });
    },
    availableTabs() {
      const tabs = [];
      if (this.description) tabs.push({ id: 'description', label: 'Descrição do Produto' });
      if (this.parsedSpecifications.length) tabs.push({ id: 'specs', label: 'Ficha Técnica' });
      if (this.measures) tabs.push({ id: 'measures', label: 'Dimensões e Peso' });
      return tabs;
    },
    hasAnyContent() {
      return this.availableTabs.length > 0;
    }
  }
};
</script>
